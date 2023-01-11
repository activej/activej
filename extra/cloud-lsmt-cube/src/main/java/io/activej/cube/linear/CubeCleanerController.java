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

package io.activej.cube.linear;

import io.activej.aggregation.AggregationChunkStorage_Reactive;
import io.activej.common.ApplicationSettings;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.cube.exception.CubeException;
import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static io.activej.cube.linear.Utils.loadResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;

public final class CubeCleanerController implements ConcurrentJmxBean, WithInitializer<CubeCleanerController> {
	private static final Logger logger = LoggerFactory.getLogger(CubeCleanerController.class);

	public static final Duration CHUNKS_CLEANUP_DELAY = ApplicationSettings.getDuration(CubeCleanerController.class, "cleanupDelay", Duration.ofMinutes(1));
	public static final Duration CLEANUP_OLDER_THAN = ApplicationSettings.getDuration(CubeCleanerController.class, "cleanupOlderThan", Duration.ofMinutes(10));
	public static final int MINIMAL_REVISIONS = ApplicationSettings.getInt(CubeCleanerController.class, "minimalRevisions", 1);

	public static final String REVISION_TABLE = ApplicationSettings.getString(CubeCleanerController.class, "revisionTable", "cube_revision");
	public static final String POSITION_TABLE = ApplicationSettings.getString(CubeCleanerController.class, "positionTable", "cube_position");
	public static final String CHUNK_TABLE = ApplicationSettings.getString(CubeCleanerController.class, "chunkTable", "cube_chunk");

	private static final String SQL_CLEANUP_SCRIPT = "sql/cleanup.sql";

	private final DataSource dataSource;
	private final ChunksCleanerService chunksCleanerService;

	private Duration chunksCleanupDelay = CHUNKS_CLEANUP_DELAY;
	private Duration cleanupOlderThan = CLEANUP_OLDER_THAN;
	private int minimalNumberOfRevisions = MINIMAL_REVISIONS;

	private String tableRevision = REVISION_TABLE;
	private String tablePosition = POSITION_TABLE;
	private String tableChunk = CHUNK_TABLE;

	// region JMX
	private long cleanupLastStartTimestamp;
	private long cleanupLastCompleteTimestamp;
	private long cleanupDurationMillis;
	private @Nullable Exception cleanupException;

	private long cleanupConsolidatedChunksLastStartTimestamp;
	private long cleanupConsolidatedChunksLastCompleteTimestamp;
	private long cleanupConsolidatedChunksDurationMillis;
	private @Nullable Exception cleanupConsolidatedChunksException;

	private long getRequiredChunksLastStartTimestamp;
	private long getRequiredChunksLastCompleteTimestamp;
	private long getRequiredChunksDurationMillis;
	private @Nullable Exception getRequiredChunksException;

	private long checkRequiredChunksLastStartTimestamp;
	private long checkRequiredChunksLastCompleteTimestamp;
	private long checkRequiredChunksDurationMillis;
	private @Nullable Exception checkRequiredChunksException;

	private long cleanupChunksLastStartTimestamp;
	private long cleanupChunksLastCompleteTimestamp;
	private long cleanupChunksDurationMillis;
	private @Nullable Exception cleanupChunksException;
	// endregion

	private CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private CubeCleanerController(DataSource dataSource, ChunksCleanerService chunksCleanerService) {
		this.dataSource = dataSource;
		this.chunksCleanerService = chunksCleanerService;
	}

	public static CubeCleanerController create(DataSource dataSource, ChunksCleanerService chunksCleanerService) {
		return new CubeCleanerController(dataSource, chunksCleanerService);
	}

	public CubeCleanerController withChunksCleanupDelay(Duration chunksCleanupDelay) {
		this.chunksCleanupDelay = chunksCleanupDelay;
		return this;
	}

	public CubeCleanerController withCurrentTimeProvider(CurrentTimeProvider now) {
		this.now = now;
		return this;
	}

	public CubeCleanerController withCustomTableNames(String tableRevision, String tablePosition, String tableChunk) {
		this.tableRevision = tableRevision;
		this.tablePosition = tablePosition;
		this.tableChunk = tableChunk;
		return this;
	}

	/**
	 * Number of revisions that should not be cleaned up if possible
	 */
	public CubeCleanerController withMinimalNumberOfRevisions(int minimalRevisions) {
		this.minimalNumberOfRevisions = minimalRevisions;
		return this;
	}

	public CubeCleanerController withCleanupOlderThen(Duration cleanupOlderThan) {
		this.cleanupOlderThan = cleanupOlderThan;
		return this;
	}

	public void cleanup() throws CubeException {
		cleanupLastStartTimestamp = now.currentTimeMillis();

		try {
			Set<Long> requiredChunks;
			try (Connection connection = dataSource.getConnection()) {
				connection.setAutoCommit(false);
				connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

				cleanupConsolidatedChunks(connection);
				requiredChunks = getRequiredChunks(connection);
			} catch (SQLException e) {
				throw new CubeException("Failed to connect to the database", e);
			}

			logger.trace("Required chunks: {}", requiredChunks);

			checkRequiredChunks(requiredChunks);
			cleanupChunks(requiredChunks);
		} catch (CubeException e) {
			cleanupException = e;
			cleanupLastCompleteTimestamp = now.currentTimeMillis();
			cleanupDurationMillis = cleanupLastCompleteTimestamp - cleanupLastStartTimestamp;
			throw e;
		}

		cleanupException = null;
		cleanupLastCompleteTimestamp = now.currentTimeMillis();
		cleanupDurationMillis = cleanupLastCompleteTimestamp - cleanupLastStartTimestamp;

		logger.trace("Chunks successfully cleaned up");
	}

	private void cleanupConsolidatedChunks(Connection connection) throws CubeException {
		logger.trace("Cleaning up consolidated chunks");

		cleanupConsolidatedChunksLastStartTimestamp = now.currentTimeMillis();

		try (Statement statement = connection.createStatement()) {
			String cleanupScript = sql(new String(loadResource(SQL_CLEANUP_SCRIPT), UTF_8));
			statement.execute(cleanupScript);

			connection.commit();
		} catch (SQLException | IOException e) {
			CubeException exception = new CubeException("Failed to clean up consolidated chunks", e);
			cleanupConsolidatedChunksException = exception;
			throw exception;
		} finally {
			cleanupConsolidatedChunksLastCompleteTimestamp = now.currentTimeMillis();
			cleanupConsolidatedChunksDurationMillis = cleanupConsolidatedChunksLastCompleteTimestamp - cleanupConsolidatedChunksLastStartTimestamp;
		}

		cleanupConsolidatedChunksException = null;

		logger.trace("Consolidated chunks have been cleaned up from the database");
	}

	private Set<Long> getRequiredChunks(Connection connection) throws CubeException {
		getRequiredChunksLastStartTimestamp = now.currentTimeMillis();

		Set<Long> requiredChunks = new HashSet<>();

		try (PreparedStatement ps = connection.prepareStatement((sql("" +
				"SELECT `id` FROM {chunk}"
		)))) {
			ResultSet resultSet = ps.executeQuery();

			while (resultSet.next()) {
				requiredChunks.add(resultSet.getLong(1));
			}
		} catch (SQLException e) {
			CubeException exception = new CubeException("Failed to retrieve required chunks", e);
			getRequiredChunksException = exception;
			throw exception;
		} finally {
			getRequiredChunksLastCompleteTimestamp = now.currentTimeMillis();
			getRequiredChunksDurationMillis = getRequiredChunksLastCompleteTimestamp - getRequiredChunksLastStartTimestamp;
		}

		getRequiredChunksException = null;

		return requiredChunks;
	}

	private void checkRequiredChunks(Set<Long> requiredChunks) throws CubeException {
		checkRequiredChunksLastStartTimestamp = now.currentTimeMillis();

		try {
			chunksCleanerService.checkRequiredChunks(requiredChunks);
		} catch (IOException e) {
			CubeException exception = new CubeException("Failed to check required chunks", e);
			checkRequiredChunksException = exception;
			throw exception;
		} finally {
			checkRequiredChunksLastCompleteTimestamp = now.currentTimeMillis();
			checkRequiredChunksDurationMillis = checkRequiredChunksLastCompleteTimestamp - checkRequiredChunksLastStartTimestamp;
		}

		checkRequiredChunksException = null;
	}

	private void cleanupChunks(Set<Long> requiredChunks) throws CubeException {
		cleanupChunksLastStartTimestamp = now.currentTimeMillis();

		try {
			chunksCleanerService.cleanup(requiredChunks, now.currentInstant().minus(chunksCleanupDelay));
		} catch (IOException e) {
			CubeException exception = new CubeException("Failed to cleanup", e);
			cleanupChunksException = exception;
			throw exception;
		} finally {
			cleanupChunksLastCompleteTimestamp = now.currentTimeMillis();
			cleanupChunksDurationMillis = cleanupChunksLastCompleteTimestamp - cleanupChunksLastStartTimestamp;
		}

		cleanupChunksException = null;
	}

	private String sql(String sql) {
		return sql
				.replace("{revision}", tableRevision)
				.replace("{position}", tablePosition)
				.replace("{chunk}", tableChunk)
				.replace("{min_revisions}", String.valueOf(minimalNumberOfRevisions))
				.replace("{cleanup_from}", String.valueOf(cleanupOlderThan.getSeconds()));
	}

	// region JMX getters
	@JmxAttribute
	public @Nullable Instant getCleanupLastStartTime() {
		return cleanupLastStartTimestamp != 0L ? Instant.ofEpochMilli(cleanupLastStartTimestamp) : null;
	}

	@JmxAttribute
	public @Nullable Instant getCleanupLastCompleteTime() {
		return cleanupLastCompleteTimestamp != 0L ? Instant.ofEpochMilli(cleanupLastCompleteTimestamp) : null;
	}

	@JmxAttribute
	public @Nullable Duration getCleanupCurrentDuration() {
		return cleanupLastStartTimestamp - cleanupLastCompleteTimestamp > 0 ?
				Duration.ofMillis(now.currentTimeMillis() - cleanupLastStartTimestamp) :
				null;
	}

	@JmxAttribute
	public Duration getCleanupLastDuration() {
		return Duration.ofMillis(cleanupDurationMillis);
	}

	@JmxAttribute(optional = true)
	public @Nullable Exception getCleanupLastException() {
		return cleanupException;
	}

	@JmxAttribute
	public @Nullable Instant getCleanupConsolidatedChunksLastStartTime() {
		return cleanupConsolidatedChunksLastStartTimestamp != 0L ? Instant.ofEpochMilli(cleanupConsolidatedChunksLastStartTimestamp) : null;
	}

	@JmxAttribute
	public @Nullable Instant getCleanupConsolidatedChunksLastCompleteTime() {
		return cleanupConsolidatedChunksLastCompleteTimestamp != 0L ?
				Instant.ofEpochMilli(cleanupConsolidatedChunksLastCompleteTimestamp) :
				null;
	}

	@JmxAttribute
	public @Nullable Duration getCleanupConsolidatedChunksCurrentDuration() {
		return cleanupConsolidatedChunksLastStartTimestamp - cleanupConsolidatedChunksLastCompleteTimestamp > 0 ?
				Duration.ofMillis(now.currentTimeMillis() - cleanupConsolidatedChunksLastStartTimestamp) :
				null;
	}

	@JmxAttribute
	public Duration getCleanupConsolidatedChunksLastDuration() {
		return Duration.ofMillis(cleanupConsolidatedChunksDurationMillis);
	}

	@JmxAttribute(optional = true)
	public @Nullable Exception getCleanupConsolidatedChunksLastException() {
		return cleanupConsolidatedChunksException;
	}

	@JmxAttribute
	public @Nullable Instant getGetRequiredChunksLastStartTime() {
		return getRequiredChunksLastStartTimestamp != 0L ? Instant.ofEpochMilli(getRequiredChunksLastStartTimestamp) : null;
	}

	@JmxAttribute
	public @Nullable Instant getGetRequiredChunksLastCompleteTime() {
		return getRequiredChunksLastCompleteTimestamp != 0L ?
				Instant.ofEpochMilli(getRequiredChunksLastCompleteTimestamp) :
				null;
	}

	@JmxAttribute
	public @Nullable Duration getGetRequiredChunksCurrentDuration() {
		return getRequiredChunksLastStartTimestamp - getRequiredChunksLastCompleteTimestamp > 0 ?
				Duration.ofMillis(now.currentTimeMillis() - getRequiredChunksLastStartTimestamp) :
				null;
	}

	@JmxAttribute
	public Duration getGetRequiredChunksLastDuration() {
		return Duration.ofMillis(getRequiredChunksDurationMillis);
	}

	@JmxAttribute(optional = true)
	public @Nullable Exception getGetRequiredChunksLastException() {
		return getRequiredChunksException;
	}

	@JmxAttribute
	public @Nullable Instant getCheckRequiredChunksLastStartTime() {
		return checkRequiredChunksLastStartTimestamp != 0L ? Instant.ofEpochMilli(checkRequiredChunksLastStartTimestamp) : null;
	}

	@JmxAttribute
	public @Nullable Instant getCheckRequiredChunksLastCompleteTime() {
		return checkRequiredChunksLastCompleteTimestamp != 0L ?
				Instant.ofEpochMilli(checkRequiredChunksLastCompleteTimestamp) :
				null;
	}

	@JmxAttribute
	public @Nullable Duration getCheckRequiredChunksCurrentDuration() {
		return checkRequiredChunksLastStartTimestamp - checkRequiredChunksLastCompleteTimestamp > 0 ?
				Duration.ofMillis(now.currentTimeMillis() - checkRequiredChunksLastStartTimestamp) :
				null;
	}

	@JmxAttribute
	public Duration getCheckRequiredChunksLastDuration() {
		return Duration.ofMillis(checkRequiredChunksDurationMillis);
	}

	@JmxAttribute(optional = true)
	public @Nullable Exception getCheckRequiredChunksLastException() {
		return checkRequiredChunksException;
	}

	@JmxAttribute
	public @Nullable Instant getCleanupChunksLastStartTime() {
		return cleanupChunksLastStartTimestamp != 0L ? Instant.ofEpochMilli(cleanupChunksLastStartTimestamp) : null;
	}

	@JmxAttribute
	public @Nullable Instant getCleanupChunksLastCompleteTime() {
		return cleanupChunksLastCompleteTimestamp != 0L ?
				Instant.ofEpochMilli(cleanupChunksLastCompleteTimestamp) :
				null;
	}

	@JmxAttribute
	public @Nullable Duration getCleanupChunksCurrentDuration() {
		return cleanupChunksLastStartTimestamp - cleanupChunksLastCompleteTimestamp > 0 ?
				Duration.ofMillis(now.currentTimeMillis() - cleanupChunksLastStartTimestamp) :
				null;
	}

	@JmxAttribute
	public Duration getCleanupChunksLastDuration() {
		return Duration.ofMillis(cleanupChunksDurationMillis);
	}

	@JmxAttribute(optional = true)
	public @Nullable Exception getCleanupChunksLastException() {
		return cleanupChunksException;
	}
	// endregion

	public interface ChunksCleanerService {
		void checkRequiredChunks(Set<Long> chunkIds) throws IOException;

		void cleanup(Set<Long> chunkIds, Instant safePoint) throws IOException;

		static ChunksCleanerService ofReactiveAggregationChunkStorage(AggregationChunkStorage_Reactive<Long> storage) {
			return Utils.cleanerServiceOfStorage(storage);
		}
	}

}
