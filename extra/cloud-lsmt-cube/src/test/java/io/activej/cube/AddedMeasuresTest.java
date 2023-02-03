package io.activej.cube;

import io.activej.aggregation.*;
import io.activej.aggregation.annotation.Key;
import io.activej.aggregation.annotation.Measures;
import io.activej.aggregation.fieldtype.FieldTypes;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.ref.RefLong;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.csp.process.frame.impl.LZ4;
import io.activej.cube.Cube.AggregationConfig;
import io.activej.cube.exception.QueryException;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.StreamSupplier;
import io.activej.fs.FileSystem;
import io.activej.reactor.Reactor;
import io.activej.record.Record;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.aggregation.fieldtype.FieldTypes.ofDouble;
import static io.activej.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.aggregation.measure.Measures.*;
import static io.activej.common.Utils.first;
import static io.activej.cube.Cube.AggregationConfig.id;
import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class AddedMeasuresTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final String AGGREGATION_ID = "test";

	private Executor executor;
	private Reactor reactor;
	private DefiningClassLoader classLoader;
	private IAggregationChunkStorage<Long> aggregationChunkStorage;
	private AggregationConfig basicConfig;
	private List<CubeDiff> initialDiffs;

	@Before
	public void setUp() throws Exception {
		executor = Executors.newCachedThreadPool();
		reactor = Reactor.getCurrentReactor();
		classLoader = DefiningClassLoader.create();
		Path path = temporaryFolder.newFolder().toPath();
		FileSystem fs = FileSystem.create(reactor, executor, path);
		await(fs.start());
		FrameFormat frameFormat = FrameFormats.lz4();
		aggregationChunkStorage = AggregationChunkStorage.create(reactor, ChunkIdJsonCodec.ofLong(), AsyncSupplier.of(new RefLong(0)::inc), frameFormat, fs);
		basicConfig = id(AGGREGATION_ID)
				.withDimensions("siteId")
				.withMeasures("eventCount", "sumRevenue", "minRevenue", "maxRevenue");

		Cube basicCube = Cube.builder(reactor, executor, classLoader, aggregationChunkStorage)
				.withDimension("siteId", FieldTypes.ofInt())
				.withMeasure("eventCount", count(ofLong()))
				.withMeasure("sumRevenue", sum(ofDouble()))
				.withMeasure("minRevenue", min(ofDouble()))
				.withMeasure("maxRevenue", max(ofDouble()))
				.withAggregation(basicConfig)
				.build();

		StreamSupplier<EventRecord> supplier = StreamSupplier.of(
				new EventRecord(1, 0.34),
				new EventRecord(10, 0.42),
				new EventRecord(4, 0.13));
		initialDiffs = new ArrayList<>();

		CubeDiff diff = await(supplier.streamTo(basicCube.consume(EventRecord.class)));
		initialDiffs.add(diff);
		aggregationChunkStorage.finish(diff.get(AGGREGATION_ID).getAddedChunks().stream().map(AggregationChunk::getChunkId).map(id -> (long) id).collect(toSet()));
		basicCube.apply(diff);

		supplier = StreamSupplier.of(
				new EventRecord(3, 0.30),
				new EventRecord(33, 0.22),
				new EventRecord(21, 0.91));

		diff = await(supplier.streamTo(basicCube.consume(EventRecord.class)));
		initialDiffs.add(diff);
		aggregationChunkStorage.finish(diff.get(AGGREGATION_ID).getAddedChunks().stream().map(AggregationChunk::getChunkId).map(id -> (long) id).collect(toSet()));
		basicCube.apply(diff);

		supplier = StreamSupplier.of(
				new EventRecord(42, 0.01),
				new EventRecord(12, 0.88),
				new EventRecord(33, 1.01));

		diff = await(supplier.streamTo(basicCube.consume(EventRecord.class)));
		initialDiffs.add(diff);
		aggregationChunkStorage.finish(diff.get(AGGREGATION_ID).getAddedChunks().stream().map(AggregationChunk::getChunkId).map(id -> (long) id).collect(toSet()));
		basicCube.apply(diff);
	}

	@Test
	public void consolidation() {
		Cube cube = Cube.builder(reactor, executor, classLoader, aggregationChunkStorage)
				.withDimension("siteId", FieldTypes.ofInt())
				.withMeasure("eventCount", count(ofLong()))
				.withMeasure("sumRevenue", sum(ofDouble()))
				.withMeasure("minRevenue", min(ofDouble()))
				.withMeasure("maxRevenue", max(ofDouble()))
				.withMeasure("uniqueUserIds", union(ofLong()))
				.withMeasure("estimatedUniqueUserIdCount", hyperLogLog(1024))
				.withAggregation(basicConfig.withMeasures("uniqueUserIds", "estimatedUniqueUserIdCount"))
				.build();

		initialDiffs.forEach(cube::apply);

		StreamSupplier<EventRecord2> supplier = StreamSupplier.of(
				new EventRecord2(14, 0.35, 500),
				new EventRecord2(12, 0.59, 17),
				new EventRecord2(22, 0.85, 50));

		CubeDiff diff = await(supplier.streamTo(cube.consume(EventRecord2.class)));
		await(aggregationChunkStorage.finish(diff.addedChunks().map(id -> (long) id).collect(toSet())));
		cube.apply(diff);

		CubeDiff cubeDiff = await(cube.consolidate(Aggregation::consolidateHotSegment));
		assertEquals(Set.of("test"), cubeDiff.keySet());
		AggregationDiff aggregationDiff = cubeDiff.get("test");

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
		Cube cube = Cube.builder(reactor, executor, classLoader, aggregationChunkStorage)
				.withDimension("siteId", FieldTypes.ofInt())
				.withMeasure("eventCount", count(ofLong()))
				.withMeasure("sumRevenue", sum(ofDouble()))
				.withMeasure("minRevenue", min(ofDouble()))
				.withMeasure("maxRevenue", max(ofDouble()))
				.withMeasure("uniqueUserIds", union(ofLong()))
				.withMeasure("estimatedUniqueUserIdCount", hyperLogLog(1024))
				.withAggregation(basicConfig.withMeasures("uniqueUserIds", "estimatedUniqueUserIdCount"))
				.build();

		initialDiffs.forEach(cube::apply);

		List<String> measures = List.of("eventCount", "estimatedUniqueUserIdCount");
		QueryResult queryResult = await(cube.query(CubeQuery.builder()
				.withMeasures(measures)
				.build()));

		assertEquals(measures, queryResult.getMeasures());
		assertEquals(1, queryResult.getRecords().size());
		Record record = first(queryResult.getRecords());
		assertEquals(9, (long) record.get("eventCount"));
	}

	@Test
	public void secondAggregation() throws QueryException {
		Cube cube = Cube.builder(reactor, executor, classLoader, aggregationChunkStorage)
				.withDimension("siteId", FieldTypes.ofInt())
				.withMeasure("eventCount", count(ofLong()))
				.withMeasure("sumRevenue", sum(ofDouble()))
				.withMeasure("minRevenue", min(ofDouble()))
				.withMeasure("maxRevenue", max(ofDouble()))
				.withMeasure("customRevenue", max(ofDouble()))
				.withMeasure("uniqueUserIds", union(ofLong()))
				.withMeasure("estimatedUniqueUserIdCount", hyperLogLog(1024))
				.withAggregation(basicConfig.withMeasures("uniqueUserIds", "customRevenue"))
				.withAggregation(AggregationConfig.id("second")
						.withDimensions("siteId")
						.withMeasures("eventCount", "sumRevenue", "minRevenue", "maxRevenue", "estimatedUniqueUserIdCount"))
				.build();

		initialDiffs.forEach(cube::apply);

		StreamSupplier<EventRecord2> supplier = StreamSupplier.of(
				new EventRecord2(14, 0.35, 500),
				new EventRecord2(12, 0.59, 17),
				new EventRecord2(22, 0.85, 50));

		CubeDiff diff = await(supplier.streamTo(cube.consume(EventRecord2.class)));
		await(aggregationChunkStorage.finish(diff.addedChunks().map(id -> (long) id).collect(toSet())));
		cube.apply(diff);

		List<String> measures = List.of("eventCount", "customRevenue", "estimatedUniqueUserIdCount");
		QueryResult queryResult = await(cube.query(CubeQuery.builder()
				.withMeasures(measures)
				.build()));

		assertEquals(measures, queryResult.getMeasures());
		assertEquals(1, queryResult.getRecords().size());
		Record record = first(queryResult.getRecords());
		assertEquals(12, (long) record.get("eventCount"));
		assertEquals(0.0, record.get("customRevenue"), 1e-10);
		assertEquals(3, (int) record.get("estimatedUniqueUserIdCount"));
	}

	@Measures("eventCount")
	public record EventRecord(@Key int siteId,
							  @Measures({"sumRevenue", "minRevenue", "maxRevenue"}) double revenue) {
	}

	@Measures("eventCount")
	public record EventRecord2(@Key int siteId,
							   @Measures({"sumRevenue", "minRevenue", "maxRevenue"}) double revenue,
							   @Measures({"uniqueUserIds", "estimatedUniqueUserIdCount"}) long userId) {
	}
}
