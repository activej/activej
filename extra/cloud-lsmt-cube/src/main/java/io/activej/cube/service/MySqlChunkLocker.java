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

import io.activej.common.ApplicationSettings;
import io.activej.common.builder.AbstractBuilder;
import io.activej.cube.aggregation.ChunkIdJsonCodec;
import io.activej.cube.aggregation.ChunksAlreadyLockedException;
import io.activej.cube.aggregation.IChunkLocker;
import io.activej.cube.linear.CubeSqlNaming;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;
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
import static java.lang.String.join;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.util.Collections.nCopies;

public final class MySqlChunkLocker<C> extends AbstractReactive
	implements IChunkLocker<C> {

	private static final Logger logger = LoggerFactory.getLogger(MySqlChunkLocker.class);

	public static final Duration DEFAULT_LOCK_TTL = ApplicationSettings.getDuration(MySqlChunkLocker.class, "lockTtl", Duration.ofMinutes(5));
	public static final @Nullable String DEFAULT_LOCKED_BY = ApplicationSettings.getString(MySqlChunkLocker.class, "lockedBy", null);

	private final Executor executor;
	private final DataSource dataSource;
	private final ChunkIdJsonCodec<C> idCodec;
	private final String aggregationId;

	private String lockedBy = DEFAULT_LOCKED_BY == null ? UUID.randomUUID().toString() : DEFAULT_LOCKED_BY;

	private CubeSqlNaming sqlNaming = CubeSqlNaming.DEFAULT_SQL_NAMING;
	private long lockTtlSeconds = DEFAULT_LOCK_TTL.getSeconds();

	private MySqlChunkLocker(
		Reactor reactor, Executor executor, DataSource dataSource, ChunkIdJsonCodec<C> idCodec, String aggregationId
	) {
		super(reactor);
		this.executor = executor;
		this.dataSource = dataSource;
		this.idCodec = idCodec;
		this.aggregationId = aggregationId;
	}

	public static <C> MySqlChunkLocker<C> create(
		Reactor reactor, Executor executor, DataSource dataSource, ChunkIdJsonCodec<C> idCodec, String aggregationId
	) {
		return builder(reactor, executor, dataSource, idCodec, aggregationId).build();
	}

	public static <C> MySqlChunkLocker<C>.Builder builder(
		Reactor reactor, Executor executor, DataSource dataSource, ChunkIdJsonCodec<C> idCodec, String aggregationId
	) {
		return new MySqlChunkLocker<>(reactor, executor, dataSource, idCodec, aggregationId).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, MySqlChunkLocker<C>> {
		private Builder() {}

		public Builder withSqlNaming(CubeSqlNaming sqlScheme) {
			checkNotBuilt(this);
			MySqlChunkLocker.this.sqlNaming = sqlScheme;
			return this;
		}

		public Builder withLockedBy(String lockedBy) {
			checkNotBuilt(this);
			MySqlChunkLocker.this.lockedBy = lockedBy;
			return this;
		}

		public Builder withLockedTtl(Duration lockTtl) {
			checkNotBuilt(this);
			MySqlChunkLocker.this.lockTtlSeconds = lockTtl.getSeconds();
			return this;
		}

		@Override
		protected MySqlChunkLocker<C> doBuild() {
			return MySqlChunkLocker.this;
		}
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	private String sql(String sql) {
		return sqlNaming.sql(sql);
	}

	public void initialize() throws IOException, SQLException {
		logger.trace("Initializing tables");
		try (
			Connection connection = dataSource.getConnection();
			Statement statement = connection.createStatement()
		) {
			statement.execute(sql(new String(loadInitScript(), UTF_8)));
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
		try (
			Connection connection = dataSource.getConnection();
			Statement statement = connection.createStatement()
		) {
			statement.execute(sql("TRUNCATE TABLE {chunk}"));
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

					try (PreparedStatement ps = connection.prepareStatement(sql("""
						UPDATE {chunk}
						SET `locked_at`=NOW(), `locked_by`=?
						WHERE
						    `removed_revision` IS NULL AND
						    (`locked_at` IS NULL OR
						     `locked_at` <= NOW() - INTERVAL ? SECOND) AND
						     `id` IN ($ids)
						"""
						.replace("$ids", join(", ", nCopies(chunkIds.size(), "?")))))
					) {
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

					try (PreparedStatement ps = connection.prepareStatement(sql("""
						UPDATE {chunk}
						SET `locked_at`=NULL, `locked_by`=NULL
						WHERE `aggregation`=?
						  AND `removed_revision` IS NULL
						  AND `locked_by`=?
						  AND `id` IN ($ids)
						"""
						.replace("$ids", join(",", nCopies(chunkIds.size(), "?")))))
					) {
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

					try (PreparedStatement ps = connection.prepareStatement(sql("""
						SELECT `id`
						FROM {chunk}
						WHERE `aggregation`=?
						  AND (`removed_revision` IS NOT NULL OR `locked_at`>NOW()-INTERVAL ? SECOND)
						"""))
					) {
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
