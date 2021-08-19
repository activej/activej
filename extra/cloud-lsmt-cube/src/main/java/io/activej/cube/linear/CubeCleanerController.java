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

import io.activej.aggregation.ActiveFsChunkStorage;
import io.activej.common.ApplicationSettings;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.cube.exception.CubeException;
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

public final class CubeCleanerController {
	private static final Logger logger = LoggerFactory.getLogger(CubeCleanerController.class);

	public static final Duration CHUNKS_CLEANUP_DELAY = ApplicationSettings.getDuration(CubeCleanerController.class, "cleanupDelay", Duration.ofMinutes(1));
	public static final Duration CLEANUP_OLDER_THEN = ApplicationSettings.getDuration(CubeCleanerController.class, "cleanupOlderThan", Duration.ofMinutes(10));
	public static final int MINIMAL_REVISIONS = ApplicationSettings.getInt(CubeCleanerController.class, "minimalRevisions", 0);

	public static final String REVISION_TABLE = ApplicationSettings.getString(CubeCleanerController.class, "revisionTable", "revision");
	public static final String POSITION_TABLE = ApplicationSettings.getString(CubeCleanerController.class, "positionTable", "position");
	public static final String CHUNK_TABLE = ApplicationSettings.getString(CubeCleanerController.class, "chunkTable", "chunk");

	private static final String SQL_CLEANUP_SCRIPT = "sql/cleanup.sql";

	private final DataSource dataSource;
	private final ChunksCleanerService chunksCleanerService;

	private Duration chunksCleanupDelay = CHUNKS_CLEANUP_DELAY;
	private Duration cleanupOlderThan = CLEANUP_OLDER_THEN;
	private int minimalNumberOfRevisions = MINIMAL_REVISIONS;

	private String tableRevision = REVISION_TABLE;
	private String tablePosition = POSITION_TABLE;
	private String tableChunk = CHUNK_TABLE;

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
		Set<Long> requiredChunks;
		try (Connection connection = dataSource.getConnection()) {
			cleanupConsolidatedChunks(connection);
			requiredChunks = getRequiredChunks(connection);
		} catch (SQLException e) {
			throw new CubeException("Failed to connect to the database", e);
		}

		logger.trace("Required chunks: " + requiredChunks);

		try {
			chunksCleanerService.checkRequiredChunks(requiredChunks);
			chunksCleanerService.cleanup(requiredChunks, now.currentInstant().minus(chunksCleanupDelay));
		} catch (IOException e) {
			throw new CubeException("Failed to cleanup", e);
		}

		logger.trace("Chunks successfully cleaned up");
	}

	private void cleanupConsolidatedChunks(Connection connection) throws CubeException {
		logger.trace("Cleaning up consolidated chunks");

		try (Statement statement = connection.createStatement()) {
			String cleanupScript = sql(new String(loadResource(SQL_CLEANUP_SCRIPT), UTF_8));
			statement.execute(cleanupScript);
		} catch (SQLException | IOException e) {
			throw new CubeException("Failed to clean up consolidated chunks", e);
		}

		logger.trace("Consolidated chunks have been cleaned up from the database");
	}

	private Set<Long> getRequiredChunks(Connection connection) throws CubeException {
		try (PreparedStatement ps = connection.prepareStatement((sql("" +
				"SELECT `id` FROM {chunk}"
		)))) {
			ResultSet resultSet = ps.executeQuery();

			Set<Long> requiredChunks = new HashSet<>();
			while (resultSet.next()) {
				requiredChunks.add(resultSet.getLong(1));
			}
			return requiredChunks;
		} catch (SQLException e) {
			throw new CubeException("Failed to retrieve required chunks", e);
		}
	}

	private String sql(String sql) {
		return sql
				.replace("{revision}", tableRevision)
				.replace("{position}", tablePosition)
				.replace("{chunk}", tableChunk)
				.replace("{min_revisions}", String.valueOf(minimalNumberOfRevisions))
				.replace("{cleanup_from}", cleanupOlderThan.getSeconds() + " SECOND");
	}

	public interface ChunksCleanerService {
		void checkRequiredChunks(Set<Long> chunkIds) throws IOException;

		void cleanup(Set<Long> chunkIds, Instant safePoint) throws IOException;

		static ChunksCleanerService ofActiveFsChunkStorage(ActiveFsChunkStorage<Long> storage) {
			return Utils.cleanerServiceOfStorage(storage);
		}
	}

}
