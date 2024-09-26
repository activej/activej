package io.activej.cube;

import io.activej.csp.process.frame.FrameFormat;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.CubeStructure.AggregationConfig;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.AggregationChunkStorage;
import io.activej.cube.aggregation.IAggregationChunkStorage;
import io.activej.cube.aggregation.ProtoAggregationChunk;
import io.activej.cube.aggregation.annotation.Key;
import io.activej.cube.aggregation.annotation.Measures;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.fieldtype.FieldTypes;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.exception.QueryException;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.ProtoCubeDiff;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.etl.LogDiff;
import io.activej.etl.LogState;
import io.activej.fs.FileSystem;
import io.activej.json.JsonCodecs;
import io.activej.ot.StateManager;
import io.activej.record.Record;
import io.activej.serializer.def.SerializerDefs;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.util.*;

import static io.activej.common.Utils.first;
import static io.activej.cube.CubeConsolidator.ConsolidationStrategy.hotSegment;
import static io.activej.cube.CubeStructure.AggregationConfig.id;
import static io.activej.cube.TestUtils.stubChunkIdGenerator;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.*;
import static io.activej.cube.aggregation.measure.Measures.*;
import static io.activej.cube.aggregation.util.Utils.materializeProtoDiff;
import static io.activej.promise.TestUtils.await;
import static io.activej.serializer.StringFormat.UTF8;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AddedMeasuresTest extends CubeTestBase {

	private static final String AGGREGATION_ID = "test";

	private IAggregationChunkStorage aggregationChunkStorage;
	private AggregationConfig basicConfig;
	private List<CubeDiff> initialDiffs;

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();

		Path path = temporaryFolder.newFolder().toPath();
		FileSystem fs = FileSystem.create(reactor, EXECUTOR, path);
		await(fs.start());
		FrameFormat frameFormat = FrameFormats.lz4();
		aggregationChunkStorage = AggregationChunkStorage.create(reactor, stubChunkIdGenerator(), frameFormat, fs);
		basicConfig = id(AGGREGATION_ID)
			.withDimensions("siteId")
			.withMeasures("eventCount", "sumRevenue", "minRevenue", "maxRevenue");

		CubeStructure basicCubeStructure = CubeStructure.builder()
			.withDimension("siteId", FieldTypes.ofInt())
			.withMeasure("eventCount", count(ofLong()))
			.withMeasure("sumRevenue", sum(ofDouble()))
			.withMeasure("minRevenue", min(ofDouble()))
			.withMeasure("maxRevenue", max(ofDouble()))
			.withAggregation(basicConfig)
			.build();

		StreamSupplier<EventRecord> supplier = StreamSuppliers.ofValues(
			new EventRecord(1, 0.34),
			new EventRecord(10, 0.42),
			new EventRecord(4, 0.13));
		initialDiffs = new ArrayList<>();

		CubeExecutor basicCubeExecutor = CubeExecutor.create(reactor, basicCubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage);

		ProtoCubeDiff diff = await(supplier.streamTo(basicCubeExecutor.consume(EventRecord.class)));
		Set<String> protoChunkIds = diff.diffs().get(AGGREGATION_ID).addedChunks().stream()
			.map(ProtoAggregationChunk::protoChunkId)
			.collect(toSet());
		Map<String, Long> chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.create(basicCubeStructure, description);
		CubeDiff cubeDiff = materializeProtoDiff(diff, chunkIds);
		initialDiffs.add(cubeDiff);
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(cubeDiff))));

		supplier = StreamSuppliers.ofValues(
			new EventRecord(3, 0.30),
			new EventRecord(33, 0.22),
			new EventRecord(21, 0.91));

		diff = await(supplier.streamTo(basicCubeExecutor.consume(EventRecord.class)));
		protoChunkIds = diff.diffs().get(AGGREGATION_ID).addedChunks().stream()
			.map(ProtoAggregationChunk::protoChunkId)
			.collect(toSet());
		chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		cubeDiff = materializeProtoDiff(diff, chunkIds);
		initialDiffs.add(cubeDiff);
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(cubeDiff))));

		supplier = StreamSuppliers.ofValues(
			new EventRecord(42, 0.01),
			new EventRecord(12, 0.88),
			new EventRecord(33, 1.01));

		diff = await(supplier.streamTo(basicCubeExecutor.consume(EventRecord.class)));
		protoChunkIds = diff.diffs().get(AGGREGATION_ID).addedChunks().stream()
			.map(ProtoAggregationChunk::protoChunkId)
			.collect(toSet());
		chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		cubeDiff = materializeProtoDiff(diff, chunkIds);
		initialDiffs.add(cubeDiff);
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(cubeDiff))));
	}

	@Test
	public void consolidation() {
		CubeStructure cubeStructure = CubeStructure.builder()
			.withDimension("siteId", FieldTypes.ofInt())
			.withMeasure("eventCount", count(ofLong()))
			.withMeasure("sumRevenue", sum(ofDouble()))
			.withMeasure("minRevenue", min(ofDouble()))
			.withMeasure("maxRevenue", max(ofDouble()))
			.withMeasure("uniqueUserIds", union(ofLong()))
			.withMeasure("estimatedUniqueUserIdCount", hyperLogLog(1024))
			.withAggregation(basicConfig.withMeasures("uniqueUserIds", "estimatedUniqueUserIdCount"))
			.build();

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.create(cubeStructure, description);
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(initialDiffs))));

		StreamSupplier<EventRecord2> supplier = StreamSuppliers.ofValues(
			new EventRecord2(14, 0.35, 500),
			new EventRecord2(12, 0.59, 17),
			new EventRecord2(22, 0.85, 50));

		CubeExecutor cubeExecutor = CubeExecutor.create(reactor, cubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage);

		ProtoCubeDiff diff = await(supplier.streamTo(cubeExecutor.consume(EventRecord2.class)));
		Set<String> protoChunkIds = diff.addedProtoChunks().collect(toSet());
		Map<String, Long> chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		CubeDiff cubeDiff = materializeProtoDiff(diff, chunkIds);
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(cubeDiff))));

		CubeConsolidator cubeConsolidator = CubeConsolidator.create(stateManager, cubeExecutor);
		CubeDiff consolidationCubeDiff = await(cubeConsolidator.consolidate(List.copyOf(cubeStructure.getAggregationIds()), hotSegment()));
		assertEquals(Set.of("test"), consolidationCubeDiff.getDiffs().keySet());
		AggregationDiff aggregationDiff = consolidationCubeDiff.getDiffs().get("test");

		Set<Object> addedIds = aggregationDiff.getAddedChunks().stream()
			.map(AggregationChunk::getChunkId)
			.collect(toSet());
		Set<Object> removedIds = aggregationDiff.getRemovedChunks().stream()
			.map(AggregationChunk::getChunkId)
			.collect(toSet());
		assertEquals(Set.of(5L), addedIds);
		assertEquals(Set.of(1L, 2L, 3L, 4L), removedIds);

		List<String> addedMeasures = first(aggregationDiff.getAddedChunks()).getMeasures();
		Set<String> expectedMeasures = Set.of("eventCount", "sumRevenue", "minRevenue", "maxRevenue", "uniqueUserIds", "estimatedUniqueUserIdCount");
		assertEquals(expectedMeasures, new HashSet<>(addedMeasures));
	}

	@Test
	public void query() throws QueryException {
		CubeStructure cubeStructure = CubeStructure.builder()
			.withDimension("siteId", FieldTypes.ofInt())
			.withMeasure("eventCount", count(ofLong()))
			.withMeasure("sumRevenue", sum(ofDouble()))
			.withMeasure("minRevenue", min(ofDouble()))
			.withMeasure("maxRevenue", max(ofDouble()))
			.withMeasure("uniqueUserIds", union(ofLong()))
			.withMeasure("estimatedUniqueUserIdCount", hyperLogLog(1024))
			.withAggregation(basicConfig.withMeasures("uniqueUserIds", "estimatedUniqueUserIdCount"))
			.build();

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.create(cubeStructure, description);
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(initialDiffs))));

		CubeExecutor cubeExecutor = CubeExecutor.create(reactor, cubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage);
		ICubeReporting cubeReporting = CubeReporting.create(stateManager, cubeStructure, cubeExecutor);

		List<String> measures = List.of("eventCount", "estimatedUniqueUserIdCount");
		QueryResult queryResult = await(cubeReporting.query(CubeQuery.builder()
			.withMeasures(measures)
			.build()));

		assertEquals(measures, queryResult.getMeasures());
		assertEquals(1, queryResult.getRecords().size());
		Record record = first(queryResult.getRecords());
		assertEquals(9, (long) record.get("eventCount"));
	}

	@Test
	public void secondAggregation() throws QueryException {
		CubeStructure cubeStructure = CubeStructure.builder()
			.withDimension("siteId", FieldTypes.ofInt())
			.withMeasure("eventCount", count(ofLong()))
			.withMeasure("sumRevenue", sum(ofDouble()))
			.withMeasure("minRevenue", min(ofDouble()))
			.withMeasure("maxRevenue", max(ofDouble()))
			.withMeasure("customRevenue", max(ofDouble()))
			.withMeasure("uniqueUserIds", union(ofLong()))
			.withMeasure("estimatedUniqueUserIdCount", hyperLogLog(1024))
			.withAggregation(basicConfig.withMeasures("uniqueUserIds", "customRevenue"))
			.withAggregation(id("second")
				.withDimensions("siteId")
				.withMeasures("eventCount", "sumRevenue", "minRevenue", "maxRevenue", "estimatedUniqueUserIdCount"))
			.build();

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.create(cubeStructure, description);
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(initialDiffs))));

		StreamSupplier<EventRecord2> supplier = StreamSuppliers.ofValues(
			new EventRecord2(14, 0.35, 500),
			new EventRecord2(12, 0.59, 17),
			new EventRecord2(22, 0.85, 50));

		CubeExecutor cubeExecutor = CubeExecutor.create(reactor, cubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage);

		ProtoCubeDiff diff = await(supplier.streamTo(cubeExecutor.consume(EventRecord2.class)));
		Set<String> protoChunkIds = diff.addedProtoChunks().collect(toSet());
		Map<String, Long> chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(materializeProtoDiff(diff, chunkIds)))));

		ICubeReporting cubeReporting = CubeReporting.create(stateManager, cubeStructure, cubeExecutor);

		List<String> measures = List.of("eventCount", "customRevenue", "estimatedUniqueUserIdCount");
		QueryResult queryResult = await(cubeReporting.query(CubeQuery.builder()
			.withMeasures(measures)
			.build()));

		assertEquals(measures, queryResult.getMeasures());
		assertEquals(1, queryResult.getRecords().size());
		Record record = first(queryResult.getRecords());
		assertEquals(12, (long) record.get("eventCount"));
		assertEquals(0.0, record.get("customRevenue"), 1e-10);
		assertEquals(3, (int) record.get("estimatedUniqueUserIdCount"));
	}

	@Test
	public void minAndMaxShortMeasures() throws QueryException {
		CubeStructure cubeStructure = CubeStructure.builder()
			.withDimension("siteId", FieldTypes.ofInt())
			.withMeasure("eventCount", count(ofLong()))
			.withMeasure("sumRevenue", sum(ofDouble()))
			.withMeasure("minRevenue", min(ofDouble()))
			.withMeasure("maxRevenue", max(ofDouble()))
			.withMeasure("customRevenue", max(ofDouble()))
			.withMeasure("minRevenueShort", min(ofShort()))
			.withMeasure("maxRevenueShort", max(ofShort()))
			.withAggregation(basicConfig)
			.withAggregation(AggregationConfig.id("minMax")
				.withDimensions("siteId")
				.withMeasures("minRevenueShort", "maxRevenueShort"))
			.build();

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.create(cubeStructure, description);
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(initialDiffs))));

		StreamSupplier<EventRecord3> supplier = StreamSuppliers.ofValues(
			new EventRecord3(14, (short) 500),
			new EventRecord3(12, (short) 17),
			new EventRecord3(22, (short) 50));

		CubeExecutor cubeExecutor = CubeExecutor.create(reactor, cubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage);

		ProtoCubeDiff diff = await(supplier.streamTo(cubeExecutor.consume(EventRecord3.class)));
		Set<String> protoChunkIds = diff.addedProtoChunks().collect(toSet());
		Map<String, Long> chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(materializeProtoDiff(diff, chunkIds)))));

		ICubeReporting cubeReporting = CubeReporting.create(stateManager, cubeStructure, cubeExecutor);

		List<String> measures = List.of("minRevenueShort", "maxRevenueShort", "eventCount", "minRevenue", "maxRevenue");
		QueryResult queryResult = await(cubeReporting.query(CubeQuery.builder()
			.withMeasures(measures)
			.build()));

		assertEquals(measures, queryResult.getMeasures());
		assertEquals(1, queryResult.getRecords().size());
		Record record = first(queryResult.getRecords());
		assertEquals(12, (long) record.get("eventCount"));

		assertEquals(short.class, record.getScheme().getFieldType("minRevenueShort"));
		assertEquals(17, (short) record.get("minRevenueShort"));

		assertEquals(short.class, record.getScheme().getFieldType("maxRevenueShort"));
		assertEquals(500, (short) record.get("maxRevenueShort"));
	}

	@Test
	public void minAndMaxByteWrappedMeasures() throws QueryException {
		CubeStructure cubeStructure = CubeStructure.builder()
			.withDimension("siteId", FieldTypes.ofInt())
			.withMeasure("eventCount", count(ofLong()))
			.withMeasure("sumRevenue", sum(ofDouble()))
			.withMeasure("minRevenue", min(ofDouble()))
			.withMeasure("maxRevenue", max(ofDouble()))
			.withMeasure("customRevenue", max(ofDouble()))
			.withMeasure("minRevenueByte", min(ofByteWrapped()))
			.withMeasure("maxRevenueByte", max(ofByteWrapped()))
			.withAggregation(basicConfig)
			.withAggregation(AggregationConfig.id("minMax")
				.withDimensions("siteId")
				.withMeasures("minRevenueByte", "maxRevenueByte"))
			.build();

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.create(cubeStructure, description);
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(initialDiffs))));

		StreamSupplier<EventRecord4> supplier = StreamSuppliers.ofValues(
			new EventRecord4(14, (byte) 101),
			new EventRecord4(12, (byte) 17),
			new EventRecord4(22, (byte) 50));

		CubeExecutor cubeExecutor = CubeExecutor.create(reactor, cubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage);

		ProtoCubeDiff diff = await(supplier.streamTo(cubeExecutor.consume(EventRecord4.class)));
		Set<String> protoChunkIds = diff.addedProtoChunks().collect(toSet());
		Map<String, Long> chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(materializeProtoDiff(diff, chunkIds)))));

		ICubeReporting cubeReporting = CubeReporting.create(stateManager, cubeStructure, cubeExecutor);

		List<String> measures = List.of("minRevenueByte", "maxRevenueByte", "eventCount", "minRevenue", "maxRevenue");
		QueryResult queryResult = await(cubeReporting.query(CubeQuery.builder()
			.withMeasures(measures)
			.build()));

		assertEquals(measures, queryResult.getMeasures());
		assertEquals(1, queryResult.getRecords().size());
		Record record = first(queryResult.getRecords());
		assertEquals(12, (long) record.get("eventCount"));

		assertEquals(Byte.class, record.getScheme().getFieldType("minRevenueByte"));
		assertEquals(17, (byte) record.get("minRevenueByte"));

		assertEquals(Byte.class, record.getScheme().getFieldType("maxRevenueByte"));
		assertEquals(101, (byte) record.get("maxRevenueByte"));
	}

	@Test
	public void lastNotNullMeasure() throws QueryException {
		CubeStructure cubeStructure = CubeStructure.builder()
			.withDimension("siteId", FieldTypes.ofInt())
			.withMeasure("val", lastNotNull(ofNullableString(), reactor))
			.withAggregation(AggregationConfig.id("lastNotNull")
				.withDimensions("siteId")
				.withMeasures("val"))
			.build();

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.create(cubeStructure, description);

		StreamSupplier<EventRecord5> supplier = StreamSuppliers.ofValues(
			new EventRecord5(1, null),
			new EventRecord5(1, "1"),
			new EventRecord5(1, null),
			new EventRecord5(2, null),
			new EventRecord5(3, "3"),
			new EventRecord5(4, "4"),
			new EventRecord5(5, null)
		);

		CubeExecutor cubeExecutor = CubeExecutor.create(reactor, cubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage);

		ProtoCubeDiff diff = await(supplier.streamTo(cubeExecutor.consume(EventRecord5.class)));
		Set<String> protoChunkIds = diff.addedProtoChunks().collect(toSet());
		Map<String, Long> chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(materializeProtoDiff(diff, chunkIds)))));

		supplier = StreamSuppliers.ofValues(
			new EventRecord5(1, null),
			new EventRecord5(2, "2"),
			new EventRecord5(3, null),
			new EventRecord5(4, "44")
		);

		diff = await(supplier.streamTo(cubeExecutor.consume(EventRecord5.class)));
		protoChunkIds = diff.addedProtoChunks().collect(toSet());
		chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(materializeProtoDiff(diff, chunkIds)))));

		ICubeReporting cubeReporting = CubeReporting.create(stateManager, cubeStructure, cubeExecutor);

		List<String> measures = List.of("val");
		QueryResult queryResult = await(cubeReporting.query(CubeQuery.builder()
			.withAttributes("siteId")
			.withMeasures(measures)
			.build()));

		assertEquals(measures, queryResult.getMeasures());

		List<Record> records = queryResult.getRecords();
		assertEquals(5, records.size());
		Record record1 = records.get(0);
		assertEquals(1, record1.getInt("siteId"));
		assertEquals("1", record1.get("val"));

		Record record2 = records.get(1);
		assertEquals(2, record2.getInt("siteId"));
		assertEquals("2", record2.get("val"));

		Record record3 = records.get(2);
		assertEquals(3, record3.getInt("siteId"));
		assertEquals("3", record3.get("val"));

		Record record4 = records.get(3);
		assertEquals(4, record4.getInt("siteId"));
		assertEquals("44", record4.get("val"));

		Record record5 = records.get(4);
		assertEquals(5, record5.getInt("siteId"));
		assertNull(record5.get("val"));
	}

	@Test
	public void lastMeasure() throws QueryException {
		CubeStructure cubeStructure = CubeStructure.builder()
			.withDimension("siteId", FieldTypes.ofInt())
			.withMeasure("value", last(ofInt(), reactor))
			.withAggregation(AggregationConfig.id("last")
				.withDimensions("siteId")
				.withMeasures("value"))
			.build();

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.create(cubeStructure, description);

		StreamSupplier<EventRecord6> supplier = StreamSuppliers.ofValues(
			new EventRecord6(1, 0),
			new EventRecord6(1, 1),
			new EventRecord6(1, 0),
			new EventRecord6(2, 0),
			new EventRecord6(3, 3),
			new EventRecord6(4, 4),
			new EventRecord6(5, 0)
		);

		CubeExecutor cubeExecutor = CubeExecutor.create(reactor, cubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage);

		ProtoCubeDiff diff = await(supplier.streamTo(cubeExecutor.consume(EventRecord6.class)));
		Set<String> protoChunkIds = diff.addedProtoChunks().collect(toSet());
		Map<String, Long> chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(materializeProtoDiff(diff, chunkIds)))));

		supplier = StreamSuppliers.ofValues(
			new EventRecord6(1, 0),
			new EventRecord6(2, 2),
			new EventRecord6(3, 0),
			new EventRecord6(4, 44)
		);

		diff = await(supplier.streamTo(cubeExecutor.consume(EventRecord6.class)));
		protoChunkIds = diff.addedProtoChunks().collect(toSet());
		chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		await(stateManager.push(List.of(LogDiff.forCurrentPosition(materializeProtoDiff(diff, chunkIds)))));

		ICubeReporting cubeReporting = CubeReporting.create(stateManager, cubeStructure, cubeExecutor);

		List<String> measures = List.of("value");
		QueryResult queryResult = await(cubeReporting.query(CubeQuery.builder()
			.withAttributes("siteId")
			.withMeasures(measures)
			.build()));

		assertEquals(measures, queryResult.getMeasures());

		List<Record> records = queryResult.getRecords();
		assertEquals(5, records.size());
		Record record1 = records.get(0);
		assertEquals(1, record1.getInt("siteId"));
		assertEquals(0, record1.getInt("value"));

		Record record2 = records.get(1);
		assertEquals(2, record2.getInt("siteId"));
		assertEquals(2, record2.getInt("value"));

		Record record3 = records.get(2);
		assertEquals(3, record3.getInt("siteId"));
		assertEquals(0, record3.getInt("value"));

		Record record4 = records.get(3);
		assertEquals(4, record4.getInt("siteId"));
		assertEquals(44, record4.getInt("value"));

		Record record5 = records.get(4);
		assertEquals(5, record5.getInt("siteId"));
		assertEquals(0, record5.getInt("value"));
	}

	private static FieldType<Byte> ofByteWrapped() {
		return new FieldType<>(Byte.class, SerializerDefs.ofByte(false), JsonCodecs.ofByte()) {};
	}

	private static FieldType<String> ofNullableString() {
		return new FieldType<>(String.class, SerializerDefs.ofNullable(SerializerDefs.ofString(UTF8)), JsonCodecs.ofString()) {};
	}

	@Measures("eventCount")
	public record EventRecord(
		@Key int siteId,
		@Measures({"sumRevenue", "minRevenue", "maxRevenue"}) double revenue
	) {
	}

	@Measures("eventCount")
	public record EventRecord2(
		@Key int siteId,
		@Measures({"sumRevenue", "minRevenue", "maxRevenue"}) double revenue,
		@Measures({"uniqueUserIds", "estimatedUniqueUserIdCount"}) long userId
	) {
	}

	@Measures("eventCount")
	public record EventRecord3(
		@Key int siteId,
		@Measures({"minRevenueShort", "maxRevenueShort"}) short revenue
	) {
	}

	@Measures("eventCount")
	public record EventRecord4(
		@Key int siteId,
		@Measures({"minRevenueByte", "maxRevenueByte"}) Byte revenue
	) {
	}

	public record EventRecord5(
		@Key int siteId,
		@Measures("val") @Nullable String value
	) {
	}

	public record EventRecord6(
		@Key int siteId,
		@Measures("value") int value
	) {
	}
}
