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

package io.activej.aggregation;

import io.activej.common.ApplicationSettings;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.joining;

public final class ChunkLockerMySql<C> implements ChunkLocker<C> {
	private static final Logger logger = LoggerFactory.getLogger(ChunkLockerMySql.class);

	public static final String DEFAULT_LOCK_TABLE = ApplicationSettings.getString(ChunkLockerMySql.class, "lockTable", "chunk_lock");
	public static final Duration DEFAULT_LOCK_TTL = ApplicationSettings.getDuration(ChunkLockerMySql.class, "lockTtl", Duration.ofHours(1));
	public static final String DEFAULT_LOCKED_BY = ApplicationSettings.getString(ChunkLockerMySql.class, "lockedBy", null);

	private final Executor executor;
	private final DataSource dataSource;
	private final ChunkIdCodec<C> idCodec;
	private final String aggregationId;

	@Nullable
	private String lockedBy = DEFAULT_LOCKED_BY;

	private String tableLock = DEFAULT_LOCK_TABLE;
	private long lockTtlSeconds = DEFAULT_LOCK_TTL.getSeconds();

	private ChunkLockerMySql(
			Executor executor,
			DataSource dataSource,
			ChunkIdCodec<C> idCodec,
			String aggregationId
	) {
		this.executor = executor;
		this.dataSource = dataSource;
		this.idCodec = idCodec;
		this.aggregationId = aggregationId;
	}

	public static <C> ChunkLockerMySql<C> create(
			Executor executor,
			DataSource dataSource,
			ChunkIdCodec<C> idCodec,
			String aggregationId
	) {
		return new ChunkLockerMySql<>(executor, dataSource, idCodec, aggregationId);
	}

	public ChunkLockerMySql<C> withLockTableName(String tableLock) {
		this.tableLock = tableLock;
		return this;
	}

	public ChunkLockerMySql<C> withLockedBy(String lockedBy) {
		this.lockedBy = lockedBy;
		return this;
	}

	public ChunkLockerMySql<C> withLockedTtl(Duration lockTtl) {
		this.lockTtlSeconds = lockTtl.getSeconds();
		return this;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	private String sql(String sql) {
		return sql.replace("{lock}", tableLock);
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
		try (InputStream stream = classLoader.getResourceAsStream("sql/chunk_lock.sql")) {
			assert stream != null;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] buffer = new byte[4096];
			int size;
			while ((size = stream.read(buffer)) != -1) {
				baos.write(buffer, 0, size);
			}
			return baos.toByteArray();
		}
	}

	public void truncateTables() throws SQLException {
		logger.trace("Truncate tables");
		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(sql("TRUNCATE TABLE {lock}"));
			}
		}
	}

	@Override
	public Promise<Void> lockChunks(Set<C> chunkIds) {
		return Promise.ofBlockingRunnable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"INSERT INTO {lock} (`aggregation_id`, `chunk_id`, `locked_by`) " +
								"VALUES " + String.join(",", nCopies(chunkIds.size(), "(?,?,?)"))
						))) {
							int index = 1;
							for (C chunkId : chunkIds) {
								ps.setString(index++, aggregationId);
								ps.setString(index++, idCodec.toFileName(chunkId));
								ps.setString(index++, lockedBy);
							}
							ps.executeUpdate();
						}
					}
				});
	}

	@Override
	public Promise<Void> releaseChunks(Set<C> chunkIds) {
		return Promise.ofBlockingRunnable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"DELETE FROM {lock} " +
								"WHERE `aggregation_id`=? AND `chunk_id` IN " +
								nCopies(chunkIds.size(), "?").stream()
										.collect(joining(",", "(", ")")))
						)) {
							ps.setString(1, aggregationId);
							int index = 2;
							for (C chunkId : chunkIds) {
								ps.setString(index++, idCodec.toFileName(chunkId));
							}

							int released = ps.executeUpdate();

							int releasedByOther = chunkIds.size() - released;
							if (releasedByOther != 0) {
								logger.warn("{} chunks were released by someone else", releasedByOther);
							}
						}
					}
				});
	}

	@Override
	public Promise<Set<C>> getLockedChunks() {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"SELECT `chunk_id` " +
								"FROM {lock} " +
								"WHERE `aggregation_id` = ? AND `locked_at` > NOW() - INTERVAL ? SECOND"
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
