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

package io.activej.ot.repository;

import com.dslplatform.json.*;
import com.dslplatform.json.JsonReader.ReadObject;
import com.dslplatform.json.JsonWriter.WriteObject;
import com.dslplatform.json.runtime.Settings;
import io.activej.async.function.AsyncSupplier;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.initializer.WithInitializer;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanWithStats;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.ot.OTCommit;
import io.activej.ot.exception.NoCommitException;
import io.activej.ot.repository.JsonIndentUtils.OnelineOutputStream;
import io.activej.ot.system.OTSystem;
import io.activej.ot.util.IdGenerator;
import io.activej.promise.Promise;
import io.activej.promise.RetryPolicy;
import io.activej.promise.jmx.PromiseStats;
import io.activej.types.TypeT;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static com.dslplatform.json.PrettifyOutputStream.IndentType.TABS;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.checkNotNull;
import static io.activej.ot.repository.JsonIndentUtils.BYTE_STREAM;
import static io.activej.ot.repository.JsonIndentUtils.indent;
import static io.activej.promise.Promises.retry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.util.stream.Collectors.joining;

public class OTRepositoryMySql<D> implements OTRepository<Long, D>, EventloopJmxBeanWithStats, WithInitializer<OTRepositoryMySql<D>> {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);
	public static final String DEFAULT_REVISION_TABLE = "ot_revisions";
	public static final String DEFAULT_DIFFS_TABLE = "ot_diffs";
	public static final String DEFAULT_BACKUP_TABLE = "ot_revisions_backup";

	private final Eventloop eventloop;
	private final Executor executor;

	private final DataSource dataSource;
	private final IdGenerator<Long> idGenerator;

	private final OTSystem<D> otSystem;

	private final ReadObject<List<D>> decoder;
	private final WriteObject<List<D>> encoder;

	private String tableRevision = DEFAULT_REVISION_TABLE;
	private String tableDiffs = DEFAULT_DIFFS_TABLE;
	private @Nullable String tableBackup = DEFAULT_BACKUP_TABLE;

	private String createdBy = null;

	private final PromiseStats promiseCreateCommitId = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promisePush = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseGetHeads = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseHasCommit = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseLoadCommit = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseIsSnapshot = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseUpdateHeads = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseHasSnapshot = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseLoadSnapshot = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseSaveSnapshot = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	private OTRepositoryMySql(Eventloop eventloop, Executor executor, DataSource dataSource, IdGenerator<Long> idGenerator,
			OTSystem<D> otSystem, ReadObject<D> decoder, WriteObject<D> encoder) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.dataSource = dataSource;
		this.idGenerator = idGenerator;
		this.otSystem = otSystem;
		this.decoder = reader -> ((JsonReader<?>) reader).readCollection(decoder);
		this.encoder = indent((writer, value) -> writer.serialize(value, encoder));
	}

	public static <D> OTRepositoryMySql<D> create(Eventloop eventloop, Executor executor, DataSource dataSource, IdGenerator<Long> idGenerator,
			OTSystem<D> otSystem, ReadObject<D> decoder, WriteObject<D> encoder) {
		return new OTRepositoryMySql<>(eventloop, executor, dataSource, idGenerator, otSystem, decoder, encoder);
	}

	public static <D, F extends ReadObject<D> & WriteObject<D>> OTRepositoryMySql<D> create(Eventloop eventloop, Executor executor, DataSource dataSource, IdGenerator<Long> idGenerator,
			OTSystem<D> otSystem, F format) {
		return new OTRepositoryMySql<>(eventloop, executor, dataSource, idGenerator, otSystem, format, format);
	}

	public static <D> OTRepositoryMySql<D> create(Eventloop eventloop, Executor executor, DataSource dataSource, IdGenerator<Long> idGenerator,
			OTSystem<D> otSystem, TypeT<? extends D> typeT) {
		return create(eventloop, executor, dataSource, idGenerator, otSystem, typeT.getType());
	}

	public static <D> OTRepositoryMySql<D> create(Eventloop eventloop, Executor executor, DataSource dataSource, IdGenerator<Long> idGenerator,
			OTSystem<D> otSystem, Class<? extends D> diffClass) {
		return create(eventloop, executor, dataSource, idGenerator, otSystem, (Type) diffClass);
	}

	@SuppressWarnings("unchecked")
	private static <D> OTRepositoryMySql<D> create(Eventloop eventloop, Executor executor, DataSource dataSource, IdGenerator<Long> idGenerator,
			OTSystem<D> otSystem, Type diffType) {
		ReadObject<D> decoder = (ReadObject<D>) DSL_JSON.tryFindReader(diffType);
		WriteObject<D> encoder = (WriteObject<D>) DSL_JSON.tryFindWriter(diffType);
		if (decoder == null || encoder == null) {
			throw new IllegalArgumentException("Unknown type: " + diffType);
		}
		return new OTRepositoryMySql<>(eventloop, executor, dataSource, idGenerator, otSystem, decoder, encoder);
	}

	public OTRepositoryMySql<D> withCreatedBy(String createdBy) {
		this.createdBy = createdBy;
		return this;
	}

	public OTRepositoryMySql<D> withCustomTableNames(String tableRevision, String tableDiffs, @Nullable String tableBackup) {
		this.tableRevision = tableRevision;
		this.tableDiffs = tableDiffs;
		this.tableBackup = tableBackup;
		return this;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	private String sql(String sql) {
		return sql
				.replace("{revisions}", tableRevision)
				.replace("{diffs}", tableDiffs)
				.replace("{backup}", Objects.toString(tableBackup, ""));
	}

	private static <T> Promise<T> retryRollbacks(AsyncSupplier<T> id) {
		//noinspection ConditionCoveredByFurtherCondition
		return retry(id, ($, e) -> e == null || !(e instanceof SQLTransactionRollbackException),
				RetryPolicy.exponentialBackoff(Duration.ofMillis(1), Duration.ofSeconds(1)));
	}

	public void initialize() throws IOException, SQLException {
		logger.trace("Initializing tables");
		execute(dataSource, sql(new String(loadResource("sql/ot_diffs.sql"), UTF_8)));
		execute(dataSource, sql(new String(loadResource("sql/ot_revisions.sql"), UTF_8)));
		if (tableBackup != null) {
			execute(dataSource, sql(new String(loadResource("sql/ot_revisions_backup.sql"), UTF_8)));
		}
	}

	private static byte[] loadResource(String name) throws IOException {
		try (InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
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

	private static void execute(DataSource dataSource, String sql) throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(sql);
			}
		}
	}

	public void truncateTables() throws SQLException {
		logger.trace("Truncate tables");
		try (Connection connection = dataSource.getConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute(sql("TRUNCATE TABLE {diffs}"));
				statement.execute(sql("TRUNCATE TABLE {revisions}"));
			}
		}
	}

	public Promise<Long> createCommitId() {
		return idGenerator.createId();
	}

	@Override
	public Promise<OTCommit<Long, D>> createCommit(Map<Long, DiffsWithLevel<D>> parentDiffs) {
		return createCommitId()
				.map(newId -> OTCommit.of(0, newId, parentDiffs));
	}

	private static final DslJson<?> DSL_JSON = new DslJson<>(Settings.withRuntime().includeServiceLoader());
	private static final ThreadLocal<JsonWriter> WRITERS = ThreadLocal.withInitial(DSL_JSON::newWriter);
	private static final ThreadLocal<JsonReader<?>> READERS = ThreadLocal.withInitial(DSL_JSON::newReader);

	private String toJson(List<D> diffs) {
		JsonWriter jsonWriter = WRITERS.get();
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		OnelineOutputStream onelineStream = new OnelineOutputStream(byteStream);
		PrettifyOutputStream prettyStream = new PrettifyOutputStream(onelineStream, TABS, 1);
		BYTE_STREAM.set(onelineStream);
		jsonWriter.reset(prettyStream);
		encoder.write(jsonWriter, diffs);
		jsonWriter.flush();
		return byteStream.toString();
	}

	private List<D> fromJson(String json) throws MalformedDataException {
		byte[] bytes = json.getBytes(UTF_8);
		List<D> deserialized;
		try {
			JsonReader<?> jsonReader = READERS.get().process(bytes, bytes.length);
			jsonReader.getNextToken();
			deserialized = decoder.read(jsonReader);
			if (jsonReader.length() != jsonReader.getCurrentIndex()) {
				String unexpectedData = jsonReader.toString().substring(jsonReader.getCurrentIndex());
				throw new MalformedDataException("Unexpected JSON data: " + unexpectedData);
			}
			return deserialized;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	@Override
	public Promise<Void> push(Collection<OTCommit<Long, D>> commits) {
		if (commits.isEmpty()) return Promise.complete();
		return retryRollbacks(() -> doPush(commits));
	}

	private @NotNull Promise<Void> doPush(Collection<OTCommit<Long, D>> commits) {
		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(false);
						connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

						for (OTCommit<Long, D> commit : commits) {
							try (PreparedStatement statement = connection.prepareStatement(sql(
									"INSERT INTO {revisions}(`id`, `epoch`, `type`, `created_by`, `level`) VALUES (?, ?, 'INNER', ?, ?)")
							)) {
								statement.setLong(1, commit.getId());
								statement.setInt(2, commit.getEpoch());
								statement.setString(3, createdBy);
								statement.setLong(4, commit.getLevel());
								statement.executeUpdate();
							}

							for (Long parentId : commit.getParents().keySet()) {
								List<D> diff = commit.getParents().get(parentId);
								try (PreparedStatement ps = connection.prepareStatement(sql(
										"INSERT INTO {diffs}(`revision_id`, `parent_id`, `diff`) VALUES (?, ?, ?)"
								))) {
									ps.setLong(1, commit.getId());
									ps.setLong(2, parentId);
									ps.setString(3, toJson(diff));
									ps.executeUpdate();
								}
							}
						}

						connection.commit();
					}
				})
				.whenComplete(promisePush.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), commits));
	}

	@Override
	public @NotNull Promise<Void> updateHeads(Set<Long> newHeads, Set<Long> excludedHeads) {
		return retryRollbacks(() -> doUpdateHeads(newHeads, excludedHeads));
	}

	private @NotNull Promise<Void> doUpdateHeads(Set<Long> newHeads, Set<Long> excludedHeads) {
		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(false);
						connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

						updateRevisions(newHeads, connection, "HEAD");
						updateRevisions(excludedHeads, connection, "INNER");

						connection.commit();
					}
				})
				.whenComplete(promiseUpdateHeads.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), newHeads, excludedHeads));
	}

	@Override
	public @NotNull Promise<Set<Long>> getAllHeads() {
		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement ps = connection.prepareStatement(sql(
								"SELECT `id` FROM {revisions} WHERE `type`='HEAD'"
						))) {
							ResultSet resultSet = ps.executeQuery();
							Set<Long> result = new HashSet<>();
							while (resultSet.next()) {
								long id = resultSet.getLong(1);
								result.add(id);
							}
							return result;
						}
					}
				})
				.whenComplete(promiseGetHeads.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	@Override
	public @NotNull Promise<Boolean> hasCommit(@NotNull Long revisionId) {
		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"SELECT 1 " +
								"FROM {revisions} " +
								"WHERE {revisions}.`id`=? AND {revisions}.`type` IN ('HEAD', 'INNER')"
						))) {
							ps.setLong(1, revisionId);
							ResultSet resultSet = ps.executeQuery();

							return resultSet.next();
						}
					}
				})
				.whenComplete(promiseHasCommit.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), revisionId));
	}

	@Override
	public @NotNull Promise<OTCommit<Long, D>> loadCommit(@NotNull Long revisionId) {
		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						Map<Long, DiffsWithLevel<D>> parentDiffs = new HashMap<>();

						int epoch = 0;
						long timestamp = 0;

						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"SELECT " +
								" {revisions}.`epoch`," +
								" {revisions}.`level`," +
								" UNIX_TIMESTAMP({revisions}.`timestamp`) AS `timestamp`, " +
								" {diffs}.`parent_id`, " +
								" {diffs}.`diff` " +
								"FROM {revisions} " +
								"LEFT JOIN {diffs} ON {diffs}.`revision_id`={revisions}.`id` " +
								"WHERE {revisions}.`id`=? AND {revisions}.`type` IN ('HEAD', 'INNER')"
						))) {
							ps.setLong(1, revisionId);
							ResultSet resultSet = ps.executeQuery();

							while (resultSet.next()) {
								epoch = resultSet.getInt(1);
								long level = resultSet.getLong(2);
								timestamp = resultSet.getLong(3) * 1000L;
								long parentId = resultSet.getLong(4);
								String diffString = resultSet.getString(5);
								if (diffString != null) {
									List<D> diff = fromJson(diffString);
									parentDiffs.put(parentId, new DiffsWithLevel<>(level - 1, diff));
								}
							}
						}

						if (timestamp == 0) {
							throw new NoCommitException(revisionId);
						}

						return OTCommit.of(epoch, revisionId, parentDiffs)
								.withTimestamp(timestamp);
					}
				})
				.whenComplete(promiseLoadCommit.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), revisionId));
	}

	@Override
	public @NotNull Promise<Boolean> hasSnapshot(@NotNull Long revisionId) {
		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement ps = connection.prepareStatement(sql(
								"SELECT `snapshot` IS NOT NULL FROM {revisions} WHERE `id`=?"
						))) {
							ps.setLong(1, revisionId);
							ResultSet resultSet = ps.executeQuery();
							if (!resultSet.next()) return false;
							return resultSet.getBoolean(1);
						}
					}
				})
				.whenComplete(promiseHasSnapshot.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), revisionId));
	}

	@Override
	public @NotNull Promise<Optional<List<D>>> loadSnapshot(@NotNull Long revisionId) {
		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement ps = connection.prepareStatement(sql(
								"SELECT `snapshot` FROM {revisions} WHERE `id`=?"
						))) {
							ps.setLong(1, revisionId);
							ResultSet resultSet = ps.executeQuery();

							if (!resultSet.next()) return Optional.<List<D>>empty();

							String str = resultSet.getString(1);
							if (str == null) return Optional.<List<D>>empty();
							List<? extends D> snapshot = fromJson(str);
							return Optional.of(otSystem.squash(snapshot));
						}
					}
				})
				.whenComplete(promiseLoadSnapshot.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), revisionId));
	}

	@Override
	public @NotNull Promise<Void> saveSnapshot(@NotNull Long revisionId, @NotNull List<D> diffs) {
		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(true);
						connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

						String snapshot = toJson(otSystem.squash(diffs));
						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"UPDATE {revisions} SET `snapshot`=? WHERE `id`=?"
						))) {
							ps.setString(1, snapshot);
							ps.setLong(2, revisionId);
							ps.executeUpdate();
						}
					}
				})
				.whenComplete(promiseSaveSnapshot.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), revisionId, diffs));
	}

	@Override
	public Promise<Void> cleanup(Long minId) {
		return retryRollbacks(() -> doCleanup(minId));
	}

	private @NotNull Promise<Void> doCleanup(Long minId) {
		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(false);
						connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);

						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"DELETE FROM {revisions} " +
								"WHERE `type` in ('HEAD', 'INNER') AND `level` < " +
								"  (SELECT t2.`level` FROM (SELECT t.`level` FROM {revisions} t WHERE t.`id`=?) AS t2)-1"
						))) {
							ps.setLong(1, minId);
							ps.executeUpdate();
						}

						try (PreparedStatement ps = connection.prepareStatement(sql("" +
								"DELETE FROM {diffs} " +
								"WHERE NOT EXISTS (SELECT * FROM {revisions} WHERE {revisions}.`id`={diffs}.`revision_id`)"
						))) {
							ps.executeUpdate();
						}

						connection.commit();
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), minId));
	}

	@Override
	public Promise<Void> backup(OTCommit<Long, D> commit, List<D> snapshot) {
		checkNotNull(tableBackup, "Cannot backup when backup table is null");
		return Promise.ofBlocking(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement statement = connection.prepareStatement(sql(
								"INSERT INTO {backup}(`id`, `epoch`, `level`, `snapshot`) VALUES (?, ?, ?, ?)"
						))) {
							statement.setLong(1, commit.getId());
							statement.setInt(2, commit.getEpoch());
							statement.setLong(3, commit.getLevel());
							statement.setString(4, toJson(snapshot));
							statement.executeUpdate();
						}
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), commit.getId(), snapshot));
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	private void updateRevisions(Collection<Long> heads, Connection connection, String type) throws SQLException {
		if (heads.isEmpty()) return;
		try (PreparedStatement ps = connection.prepareStatement(sql("" +
				"UPDATE {revisions} " +
				"SET `type`='" + type + "' " +
				"WHERE `id` IN " + Stream.generate(() -> "?").limit(heads.size()).collect(joining(", ", "(", ")"))
		))) {
			int pos = 1;
			for (Long id : heads) {
				ps.setLong(pos++, id);
			}
			ps.executeUpdate();
		}
	}

	@JmxAttribute
	public PromiseStats getPromiseCreateCommitId() {
		return promiseCreateCommitId;
	}

	@JmxAttribute
	public PromiseStats getPromisePush() {
		return promisePush;
	}

	@JmxAttribute
	public PromiseStats getPromiseGetHeads() {
		return promiseGetHeads;
	}

	@JmxAttribute
	public PromiseStats getPromiseHasCommit() {
		return promiseHasCommit;
	}

	@JmxAttribute
	public PromiseStats getPromiseLoadCommit() {
		return promiseLoadCommit;
	}

	@JmxAttribute
	public PromiseStats getPromiseIsSnapshot() {
		return promiseIsSnapshot;
	}

	@JmxAttribute
	public PromiseStats getPromiseHasSnapshot() {
		return promiseHasSnapshot;
	}

	@JmxAttribute
	public PromiseStats getPromiseLoadSnapshot() {
		return promiseLoadSnapshot;
	}

	@JmxAttribute
	public PromiseStats getPromiseSaveSnapshot() {
		return promiseSaveSnapshot;
	}

}
