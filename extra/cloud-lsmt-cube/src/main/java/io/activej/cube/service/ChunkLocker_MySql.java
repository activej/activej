/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.cube.service;

import io.activej.aggregation.ChunkIdJsonCodec;
import io.activej.aggregation.ChunksAlreadyLockedException;
import io.activej.aggregation.IChunkLocker;
import io.activej.common.ApplicationSettings;
import io.activej.common.builder.AbstractBuilder;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;

import static io.activej.common.Checks.checkArgument;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.joining;

public final class ChunkLocker_MySql<C> extends AbstractReactive
		implements IChunkLocker<C> {
	private static final Logger logger = LoggerFactory.getLogger(ChunkLocker_MySql.class);

	public static final String CHUNK_TABLE = ApplicationSettings.getString(ChunkLocker_MySql.class, "chunkTable", "cube_chunk");
	public static final Duration DEFAULT_LOCK_TTL = ApplicationSettings.getDuration(ChunkLocker_MySql.class, "lockTtl", Duration.ofMinutes(5));
	public static final String DEFAULT_LOCKED_BY = ApplicationSettings.getString(ChunkLocker_MySql.class, "lockedBy", null);

	private final Executor executor;
	private final DataSource dataSource;
	private final ChunkIdJsonCodec<C> idCodec;
	private final String aggregationId;

	private String lockedBy = DEFAULT_LOCKED_BY == null ? UUID.randomUUID().toString() : DEFAULT_LOCKED_BY;

	private String tableChunk = CHUNK_TABLE;
	private long lockTtlSeconds = DEFAULT_LOCK_TTL.getSeconds();

	private ChunkLocker_MySql(
			Reactor reactor,
			Executor executor,
			DataSource dataSource,
			ChunkIdJsonCodec<C> idCodec,
			String aggregationId
	) {
		super(reactor);
		this.executor = executor;
		this.dataSource = dataSource;
		this.idCodec = idCodec;
		this.aggregationId = aggregationId;
	}

	public static <C> ChunkLocker_MySql<C> create(
			Reactor reactor,
			Executor executor,
			DataSource dataSource,
			ChunkIdJsonCodec<C> idCodec,
			String aggregationId
	) {
		return builder(reactor, executor, dataSource, idCodec, aggregationId).build();
	}

	public static <C> ChunkLocker_MySql<C>.Builder builder(
			Reactor reactor,
			Executor executor,
			DataSource dataSource,
			ChunkIdJsonCodec<C> idCodec,
			String aggregationId
	) {
		return new ChunkLocker_MySql<>(reactor, executor, dataSource, idCodec, aggregationId).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, ChunkLocker_MySql<C>> {
		private Builder() {}

		public Builder withLockTableName(String tableLock) {
			checkNotBuilt(this);
			ChunkLocker_MySql.this.tableChunk = tableLock;
			return this;
		}

		public Builder withLockedBy(String lockedBy) {
			checkNotBuilt(this);
			ChunkLocker_MySql.this.lockedBy = lockedBy;
			return this;
		}

		public Builder withLockedTtl(Duration lockTtl) {
			checkNotBuilt(this);
			ChunkLocker_MySql.this.lockTtlSeconds = lockTtl.getSeconds();
			return this;
		}

		@Override
		protected ChunkLocker_MySql<C> doBuild() {
			return ChunkLocker_MySql.this;
		}
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	private String sql(String sql) {
		return sql.replace("{chunk}", tableChunk);
	}

	public void initialize() throws IOException, SQLException {
		logger.trace("Initializing tables");
		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(sql(new String(loadInitScript(), UTF_8)));
			}
		}
	}

	private static byte[] loadInitScript() throws IOException {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		try (InputStream stream = classLoader.getResourceAsStream("sql/ddl/uplink_chunk.sql")) {
			assert stream != null;
			return stream.readAllBytes();
		}
	}

	public void truncateTables() throws SQLException {
		logger.trace("Truncate tables");
		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(sql("TRUNCATE TABLE {chunk}"));
			}
		}
	}

	@Override
	public Promise<Void> lockChunks(Set<C> chunkIds) {
		checkInReactorThread(this);
		checkArgument(!chunkIds.isEmpty(), "Nothing to lock");

		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(false);
						connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"UPDATE {chunk} " +
								"SET `locked_at`=NOW(), `locked_by`=?" +
								"WHERE" +
								" `removed_revision` IS NULL AND" +
								" (`locked_at` IS NULL OR" +
								" `locked_at` <= NOW() - INTERVAL ? SECOND) AND" +
								" `id` IN " +
								nCopies(chunkIds.size(), "?").stream()
										.collect(joining(",", "(", ")"))
						))) {
							ps.setString(1, lockedBy);
							ps.setLong(2, lockTtlSeconds);
							int index = 3;
							for (C chunkId : chunkIds) {
								ps.setString(index++, idCodec.toFileName(chunkId));
							}
							int updated = ps.executeUpdate();
							if (updated != chunkIds.size()) {
								throw new ChunksAlreadyLockedException();
							}
							connection.commit();
						}
					}
				});
	}

	@Override
	public Promise<Void> releaseChunks(Set<C> chunkIds) {
		checkInReactorThread(this);
		checkArgument(!chunkIds.isEmpty(), "Nothing to release");

		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(true);
						connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"UPDATE {chunk} " +
								"SET `locked_at`=NULL, `locked_by`=NULL " +
								"WHERE" +
								" `aggregation` = ? AND" +
								" `removed_revision` IS NULL AND" +
								" `locked_by`=? AND" +
								" `id` IN " +
								nCopies(chunkIds.size(), "?").stream()
										.collect(joining(",", "(", ")")))
						)) {
							ps.setString(1, aggregationId);
							ps.setString(2, lockedBy);
							int index = 3;
							for (C chunkId : chunkIds) {
								ps.setString(index++, idCodec.toFileName(chunkId));
							}

							ps.executeUpdate();
						}
					}
				});
	}

	@Override
	public Promise<Set<C>> getLockedChunks() {
		checkInReactorThread(this);
		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"SELECT `id` " +
								"FROM {chunk} " +
								"WHERE" +
								" `aggregation` = ? AND" +
								" (`removed_revision` IS NOT NULL OR `locked_at` > NOW() - INTERVAL ? SECOND)"
						))) {
							ps.setString(1, aggregationId);
							ps.setLong(2, lockTtlSeconds);

							ResultSet resultSet = ps.executeQuery();

							Set<C> result = new HashSet<>();
							while (resultSet.next()) {
								C chunkId = idCodec.fromFileName(resultSet.getString(1));
								result.add(chunkId);
							}
							return result;
						}
					}
				});
	}
}
