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

import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.util.JsonCodec;
import io.activej.common.ApplicationSettings;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.tuple.Tuple2;
import io.activej.cube.exception.StateFarAheadException;
import io.activej.cube.ot.CubeDiff;
import io.activej.etl.LogDiff;
import io.activej.etl.LogPositionDiff;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogPosition;
import io.activej.ot.exception.OTException;
import io.activej.ot.uplink.OTUplink;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.stream.LongStream;

import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.*;
import static io.activej.cube.Utils.fromJson;
import static io.activej.cube.Utils.toJson;
import static io.activej.cube.linear.Utils.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.util.Collections.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public final class CubeUplinkMySql implements OTUplink<Long, LogDiff<CubeDiff>, CubeUplinkMySql.UplinkProtoCommit> {
	private static final Logger logger = LoggerFactory.getLogger(CubeUplinkMySql.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = ApplicationSettings.getDuration(CubeUplinkMySql.class, "smoothingWindow", Duration.ofMinutes(5));
	public static final String REVISION_TABLE = ApplicationSettings.getString(CubeUplinkMySql.class, "revisionTable", "cube_revision");
	public static final String POSITION_TABLE = ApplicationSettings.getString(CubeUplinkMySql.class, "positionTable", "cube_position");
	public static final String CHUNK_TABLE = ApplicationSettings.getString(CubeUplinkMySql.class, "chunkTable", "cube_chunk");

	public static final long ROOT_REVISION = 0L;

	private static final MeasuresValidator NO_MEASURE_VALIDATION = ($1, $2) -> {};

	private final Executor executor;
	private final DataSource dataSource;

	private final PrimaryKeyCodecs primaryKeyCodecs;

	private String tableRevision = REVISION_TABLE;
	private String tablePosition = POSITION_TABLE;
	private String tableChunk = CHUNK_TABLE;

	private MeasuresValidator measuresValidator = NO_MEASURE_VALIDATION;

	private @Nullable String createdBy = null;

	// region JMX
	private final PromiseStats promiseCheckout = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseFetch = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promisePush = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	// endregion

	private CubeUplinkMySql(Executor executor, DataSource dataSource, PrimaryKeyCodecs primaryKeyCodecs) {
		this.executor = executor;
		this.dataSource = dataSource;
		this.primaryKeyCodecs = primaryKeyCodecs;
	}

	public static CubeUplinkMySql create(Executor executor, DataSource dataSource, PrimaryKeyCodecs primaryKeyCodecs) {
		return new CubeUplinkMySql(executor, dataSource, primaryKeyCodecs);
	}

	public CubeUplinkMySql withMeasuresValidator(MeasuresValidator measuresValidator) {
		this.measuresValidator = measuresValidator;
		return this;
	}

	public CubeUplinkMySql withCustomTableNames(String tableRevision, String tablePosition, String tableChunk) {
		this.tableRevision = tableRevision;
		this.tablePosition = tablePosition;
		this.tableChunk = tableChunk;
		return this;
	}

	public CubeUplinkMySql withCreatedBy(String createdBy) {
		this.createdBy = createdBy;
		return this;
	}

	@Override
	public Promise<FetchData<Long, LogDiff<CubeDiff>>> checkout() {
		return Promise.ofBlocking(executor,
						() -> {
							try (Connection connection = dataSource.getConnection()) {
								connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

								long revision = getMaxRevision(connection);

								if (revision == ROOT_REVISION) {
									return new FetchData<>(revision, revision, Collections.<LogDiff<CubeDiff>>emptyList());
								}

								List<LogDiff<CubeDiff>> diffs = doFetch(connection, ROOT_REVISION, revision);

								try (PreparedStatement ps = connection.prepareStatement(sql("" +
										"SELECT EXISTS " +
										"(SELECT * FROM {revision} WHERE `revision`=?)")
								)) {
									ps.setLong(1, revision);
									ResultSet resultSet = ps.executeQuery();
									resultSet.next();
									if (!resultSet.getBoolean(1)) {
										throw new StateFarAheadException(0L, singleton(revision));
									}
								}

								return new FetchData<>(revision, revision, diffs);
							}
						})
				.whenComplete(toLogger(logger, "checkout"))
				.whenComplete(promiseCheckout.recordStats());
	}

	@Override
	public Promise<FetchData<Long, LogDiff<CubeDiff>>> fetch(@NotNull Long currentCommitId) {
		return Promise.ofBlocking(executor,
						() -> {
							try (Connection connection = dataSource.getConnection()) {
								connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

								long revision = getMaxRevision(connection);
								if (revision == currentCommitId) {
									return new FetchData<>(revision, revision, Collections.<LogDiff<CubeDiff>>emptyList());
								}
								if (revision < currentCommitId) {
									throw new IllegalArgumentException("Passed revision is higher than uplink revision");
								}

								List<LogDiff<CubeDiff>> diffs = doFetch(connection, currentCommitId, revision);
								checkRevisions(connection, currentCommitId, revision);
								return new FetchData<>(revision, revision, diffs);
							}
						})
				.whenComplete(toLogger(logger, "fetch", currentCommitId))
				.whenComplete(promiseFetch.recordStats());
	}

	@Override
	public Promise<UplinkProtoCommit> createProtoCommit(Long parent, List<LogDiff<CubeDiff>> diffs, long parentLevel) {
		checkArgument(parent == parentLevel, "Level mismatch");

		return Promise.of(new UplinkProtoCommit(parent, diffs));
	}

	@Override
	public Promise<FetchData<Long, LogDiff<CubeDiff>>> push(UplinkProtoCommit protoCommit) {
		return Promise.ofBlocking(executor,
						() -> {
							try (Connection connection = dataSource.getConnection()) {
								connection.setAutoCommit(false);
								connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

								long revision = getMaxRevision(connection);
								if (revision < protoCommit.parentRevision) {
									throw new IllegalArgumentException("Uplink revision is less than parent revision");
								}
								while (true) {
									try (PreparedStatement ps = connection.prepareStatement(sql("" +
											"INSERT INTO {revision} (`revision`, `created_by`) VALUES (?,?)"
									))) {
										ps.setLong(1, ++revision);
										ps.setString(2, createdBy);
										ps.executeUpdate();
										logger.trace("Successfully inserted revision {}", revision);
										break;
									} catch (SQLIntegrityConstraintViolationException ignored) {
										logger.warn("Someone pushed to the same revision number {}, retry with the next revision", revision);
									}
								}

								List<LogDiff<CubeDiff>> diffsList = protoCommit.diffs;

								Set<ChunkWithAggregationId> added = collectChunks(diffsList, true);
								Set<ChunkWithAggregationId> removed = collectChunks(diffsList, false);

								// squash
								Set<ChunkWithAggregationId> intersection = intersection(added, removed);
								added.removeAll(intersection);
								removed.removeAll(intersection);

								if (!added.isEmpty()) {
									addChunks(connection, revision, added);
								}

								if (!removed.isEmpty()) {
									removeChunks(connection, revision, removed);
								}

								Map<String, LogPosition> positions = collectPositions(diffsList);
								if (!positions.isEmpty()) {
									updatePositions(connection, revision, positions);
								}

								if (revision == protoCommit.parentRevision + 1) {
									logger.trace("Nothing to fetch after diffs are pushed");
									connection.commit();
									return new FetchData<>(revision, revision, Collections.<LogDiff<CubeDiff>>emptyList());
								}

								logger.trace("Fetching diffs from finished concurrent pushes");
								List<LogDiff<CubeDiff>> diffs = doFetch(connection, protoCommit.parentRevision, revision - 1);
								checkRevisions(connection, protoCommit.parentRevision, revision - 1);
								connection.commit();
								return new FetchData<>(revision, revision, diffs);
							}
						})
				.whenComplete(toLogger(logger, "push", protoCommit))
				.whenComplete(promisePush.recordStats());
	}

	@VisibleForTesting
	CubeDiff fetchChunkDiffs(Connection connection, long from, long to) throws SQLException, MalformedDataException {
		CubeDiff cubeDiff;
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"SELECT `id`, `aggregation`, `measures`, `min_key`, `max_key`, `item_count`," +
				" ISNULL(`removed_revision`) OR `removed_revision`>? " +
				"FROM {chunk} " +
				"WHERE " +
				"(`removed_revision` BETWEEN ? AND ? AND `added_revision`<?)" +
				" OR " +
				"(`added_revision` BETWEEN ? AND ? AND (`removed_revision` IS NULL OR `removed_revision`>?))"
		))) {
			from++;
			ps.setLong(1, to);
			ps.setLong(2, from);
			ps.setLong(3, to);
			ps.setLong(4, from);
			ps.setLong(5, from);
			ps.setLong(6, to);
			ps.setLong(7, to);
			ResultSet resultSet = ps.executeQuery();

			Map<String, Tuple2<Set<AggregationChunk>, Set<AggregationChunk>>> aggregationDiffs = new HashMap<>();
			while (resultSet.next()) {
				long chunkId = resultSet.getLong(1);
				String aggregationId = resultSet.getString(2);
				List<String> measures = measuresFromString(resultSet.getString(3));
				measuresValidator.validate(aggregationId, measures);
				JsonCodec<PrimaryKey> codec = primaryKeyCodecs.getCodec(aggregationId);
				if (codec == null) {
					throw new MalformedDataException("Unknown aggregation: " + aggregationId);
				}
				PrimaryKey minKey = fromJson(codec, resultSet.getString(4));
				PrimaryKey maxKey = fromJson(codec, resultSet.getString(5));
				int count = resultSet.getInt(6);
				boolean isAdded = resultSet.getBoolean(7);

				AggregationChunk chunk = AggregationChunk.create(chunkId, measures, minKey, maxKey, count);

				Tuple2<Set<AggregationChunk>, Set<AggregationChunk>> tuple = aggregationDiffs
						.computeIfAbsent(aggregationId, $ -> new Tuple2<>(new HashSet<>(), new HashSet<>()));

				if (isAdded) {
					tuple.getValue1().add(chunk);
				} else {
					tuple.getValue2().add(chunk);
				}
			}

			cubeDiff = CubeDiff.of(transformMap(aggregationDiffs, tuple -> AggregationDiff.of(tuple.getValue1(), tuple.getValue2())));
		}
		return cubeDiff;
	}

	@VisibleForTesting
	Map<String, LogPositionDiff> fetchPositionDiffs(Connection connection, long from, long to) throws SQLException {
		Map<String, LogPositionDiff> positions = new HashMap<>();
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"SELECT p.`partition_id`, p.`filename`, p.`remainder`, p.`position`, g.`to` " +
				"FROM (SELECT `partition_id`, MAX(`revision_id`) AS `max_revision`, `revision_id`>? as `to`" +
				" FROM {position}" +
				" WHERE `revision_id`<=?" +
				" GROUP BY `partition_id`, `to`) g " +
				"LEFT JOIN" +
				" {position} p " +
				"ON p.`partition_id` = g.`partition_id` " +
				"AND p.`revision_id` = g.`max_revision` " +
				"ORDER BY p.`partition_id`, `to`"
		))) {
			ps.setLong(1, from);
			ps.setLong(2, to);

			ResultSet resultSet = ps.executeQuery();
			LogPosition[] fromTo = new LogPosition[2];
			String currentPartition = null;
			while (resultSet.next()) {
				String partition = resultSet.getString(1);
				if (!partition.equals(currentPartition)) {
					fromTo[0] = null;
					fromTo[1] = null;
				}
				currentPartition = partition;
				String filename = resultSet.getString(2);
				int remainder = resultSet.getInt(3);
				long position = resultSet.getLong(4);
				boolean isTo = resultSet.getBoolean(5);

				LogFile logFile = new LogFile(filename, remainder);
				LogPosition logPosition = LogPosition.create(logFile, position);

				if (isTo) {
					if (fromTo[0] == null) {
						fromTo[0] = LogPosition.initial();
					}
					fromTo[1] = logPosition;
				} else {
					fromTo[0] = logPosition;
					fromTo[1] = null;
					continue;
				}

				LogPositionDiff logPositionDiff = new LogPositionDiff(fromTo[0], fromTo[1]);
				positions.put(partition, logPositionDiff);
			}
		}
		return positions;
	}

	private List<LogDiff<CubeDiff>> doFetch(Connection connection, long from, long to) throws SQLException, MalformedDataException {
		return toLogDiffs(
				fetchChunkDiffs(connection, from, to),
				fetchPositionDiffs(connection, from, to)
		);
	}

	private void checkRevisions(Connection connection, long from, long to) throws SQLException, StateFarAheadException {
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"SELECT `revision` " +
				"FROM {revision} " +
				"WHERE `revision` BETWEEN ? AND ?"))
		) {
			ps.setLong(1, from);
			ps.setLong(2, to);

			ResultSet resultSet = ps.executeQuery();
			Set<Long> retrieved = new LinkedHashSet<>();
			while (resultSet.next()) {
				retrieved.add(resultSet.getLong(1));
			}
			if (retrieved.size() == to - from + 1) return;

			Set<Long> expected = LongStream.range(from, to + 1)
					.boxed()
					.collect(toSet());

			throw new StateFarAheadException(from, difference(expected, retrieved));
		}
	}

	private void addChunks(Connection connection, long newRevision, Set<ChunkWithAggregationId> chunks) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"INSERT INTO {chunk} (`id`, `aggregation`, `measures`, `min_key`, `max_key`, `item_count`, `added_revision`) " +
				"VALUES " + String.join(",", nCopies(chunks.size(), "(?,?,?,?,?,?,?)"))
		))) {
			int index = 1;
			for (ChunkWithAggregationId chunk : chunks) {
				String aggregationId = chunk.getAggregationId();
				AggregationChunk aggregationChunk = chunk.getChunk();

				ps.setLong(index++, (long) aggregationChunk.getChunkId());
				ps.setString(index++, aggregationId);
				List<String> measures = aggregationChunk.getMeasures();
				measuresValidator.validate(aggregationId, measures);
				ps.setString(index++, measuresToString(measures));
				JsonCodec<PrimaryKey> codec = primaryKeyCodecs.getCodec(aggregationId);
				if (codec == null) {
					throw new IllegalArgumentException("Unknown aggregation: " + aggregationId);
				}
				ps.setString(index++, toJson(codec, aggregationChunk.getMinPrimaryKey()));
				ps.setString(index++, toJson(codec, aggregationChunk.getMaxPrimaryKey()));
				ps.setInt(index++, aggregationChunk.getCount());
				ps.setLong(index++, newRevision);
			}

			ps.executeUpdate();
		} catch (MalformedDataException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private void removeChunks(Connection connection, long newRevision, Set<ChunkWithAggregationId> chunks) throws SQLException, OTException {
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"UPDATE {chunk} " +
				"SET `removed_revision`=? " +
				"WHERE `id` IN " +
				nCopies(chunks.size(), "?")
						.stream()
						.collect(joining(",", "(", ")"))
		))) {
			int index = 1;
			ps.setLong(index++, newRevision);
			for (ChunkWithAggregationId chunk : chunks) {
				ps.setLong(index++, (Long) chunk.getChunk().getChunkId());
			}

			int removed = ps.executeUpdate();
			if (removed != chunks.size()) {
				connection.rollback();
				throw new OTException("Chunk is already removed");
			}
		}
	}

	private void updatePositions(Connection connection, long newRevision, Map<String, LogPosition> positions) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"INSERT INTO {position} (`revision_id`, `partition_id`, `filename`, `remainder`, `position`) " +
				"VALUES " + String.join(",", nCopies(positions.size(), "(?,?,?,?,?)"))
		))) {
			int index = 1;
			for (Map.Entry<String, LogPosition> entry : positions.entrySet()) {
				LogPosition position = entry.getValue();
				LogFile logFile = position.getLogFile();

				ps.setLong(index++, newRevision);
				ps.setString(index++, entry.getKey());
				ps.setString(index++, logFile.getName());
				ps.setInt(index++, logFile.getRemainder());
				ps.setLong(index++, position.getPosition());
			}

			ps.executeUpdate();
		}
	}

	private List<LogDiff<CubeDiff>> toLogDiffs(CubeDiff cubeDiff, Map<String, LogPositionDiff> positions) {
		List<LogDiff<CubeDiff>> logDiffs;
		if (cubeDiff.isEmpty()) {
			if (positions.isEmpty()) {
				logDiffs = emptyList();
			} else {
				logDiffs = singletonList(LogDiff.of(positions, emptyList()));
			}
		} else {
			logDiffs = singletonList(LogDiff.of(positions, cubeDiff));
		}
		return logDiffs;
	}

	private String sql(String sql) {
		return sql
				.replace("{revision}", tableRevision)
				.replace("{position}", tablePosition)
				.replace("{chunk}", tableChunk);
	}

	public void initialize() throws IOException, SQLException {
		logger.trace("Initializing tables");
		executeSqlScript(dataSource, sql(new String(loadResource("sql/ddl/uplink_revision.sql"), UTF_8)));
		executeSqlScript(dataSource, sql(new String(loadResource("sql/ddl/uplink_chunk.sql"), UTF_8)));
		executeSqlScript(dataSource, sql(new String(loadResource("sql/ddl/uplink_position.sql"), UTF_8)));
	}

	public void truncateTables() throws SQLException {
		logger.trace("Truncate tables");
		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(sql("TRUNCATE TABLE {chunk}"));
				statement.execute(sql("TRUNCATE TABLE {position}"));
				statement.execute(sql("DELETE FROM {revision} WHERE `revision`!=0"));
			}
		}
	}

	private long getMaxRevision(Connection connection) throws SQLException, OTException {
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"SELECT MAX(`revision`) FROM {revision}"
		))) {
			ResultSet resultSet = ps.executeQuery();
			if (!resultSet.next()) {
				throw new OTException("Empty repository");
			}
			return resultSet.getLong(1);
		}
	}

	private static Set<ChunkWithAggregationId> collectChunks(List<LogDiff<CubeDiff>> diffsList, boolean added) {
		Set<ChunkWithAggregationId> chunks = new HashSet<>();
		for (LogDiff<CubeDiff> logDiff : diffsList) {
			for (CubeDiff cubeDiff : logDiff.getDiffs()) {
				for (Map.Entry<String, AggregationDiff> entry : cubeDiff.entrySet()) {
					String aggregationId = entry.getKey();
					AggregationDiff diff = entry.getValue();
					for (AggregationChunk chunk : added ? diff.getAddedChunks() : diff.getRemovedChunks()) {
						chunks.add(new ChunkWithAggregationId(chunk, aggregationId));
					}
				}
			}
		}
		return chunks;
	}

	private static Map<String, LogPosition> collectPositions(List<LogDiff<CubeDiff>> diffsList) {
		Map<String, LogPosition> result = new HashMap<>();
		for (LogDiff<CubeDiff> logDiff : diffsList) {
			for (Map.Entry<String, LogPositionDiff> entry : logDiff.getPositions().entrySet()) {
				result.put(entry.getKey(), entry.getValue().to);
			}
		}
		return result;
	}

	// region JMX getters
	@JmxAttribute
	public PromiseStats getPromiseCheckout() {
		return promiseCheckout;
	}

	@JmxAttribute
	public PromiseStats getPromiseFetch() {
		return promiseFetch;
	}

	@JmxAttribute
	public PromiseStats getPromisePush() {
		return promisePush;
	}
	// endregion

	public static final class UplinkProtoCommit {
		private final long parentRevision;
		private final List<LogDiff<CubeDiff>> diffs;

		public UplinkProtoCommit(long parentRevision, List<LogDiff<CubeDiff>> diffs) {
			this.parentRevision = parentRevision;
			this.diffs = diffs;
		}

		public long getParentRevision() {
			return parentRevision;
		}

		public List<LogDiff<CubeDiff>> getDiffs() {
			return diffs;
		}

		@Override
		public String toString() {
			return "{parentRevision=" + parentRevision + ", diffs=" + diffs + '}';
		}
	}
}
