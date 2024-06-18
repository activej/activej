package io.activej.cube;

import io.activej.codegen.DefiningClassLoader;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.AggregationChunkStorage;
import io.activej.cube.aggregation.IAggregationChunkStorage;
import io.activej.cube.aggregation.ProtoAggregationChunk;
import io.activej.cube.aggregation.annotation.Key;
import io.activej.cube.aggregation.annotation.Measures;
import io.activej.cube.aggregation.fieldtype.FieldTypes;
import io.activej.cube.aggregation.ot.ProtoAggregationDiff;
import io.activej.cube.aggregation.util.HyperLogLog;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.fs.FileSystem;
import io.activej.reactor.Reactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.cube.TestUtils.*;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofDouble;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.cube.aggregation.measure.Measures.*;
import static io.activej.cube.aggregation.util.Utils.materializeProtoDiff;
import static io.activej.promise.TestUtils.await;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertEquals;

public class CustomFieldsTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Measures("eventCount")
	public record EventRecord(
		@Key int siteId,
		@Measures({"sumRevenue", "minRevenue", "maxRevenue"}) double revenue,
		@Measures({"uniqueUserIds", "estimatedUniqueUserIdCount"}) long userId
	) {
	}

	public static class QueryResult {
		public int siteId;

		public long eventCount;
		public double sumRevenue;
		public double minRevenue;
		public double maxRevenue;
		public Set<Long> uniqueUserIds;
		public HyperLogLog estimatedUniqueUserIdCount;

		@Override
		public String toString() {
			return
				"QueryResult{" +
				"siteId=" + siteId +
				", eventCount=" + eventCount +
				", sumRevenue=" + sumRevenue +
				", minRevenue=" + minRevenue +
				", maxRevenue=" + maxRevenue +
				", uniqueUserIds=" + uniqueUserIds +
				", estimatedUniqueUserIdCount=" + estimatedUniqueUserIdCount +
				'}';
		}
	}

	@Test
	public void test() throws Exception {
		Executor executor = Executors.newCachedThreadPool();
		Reactor reactor = Reactor.getCurrentReactor();
		DefiningClassLoader classLoader = DefiningClassLoader.create();

		Path path = temporaryFolder.newFolder().toPath();
		FileSystem fs = FileSystem.create(reactor, executor, path);
		await(fs.start());
		FrameFormat frameFormat = FrameFormats.lz4();
		IAggregationChunkStorage aggregationChunkStorage = AggregationChunkStorage.create(reactor, stubChunkIdGenerator(), frameFormat, fs);

		AggregationStructure structure = aggregationStructureBuilder()
			.withKey("siteId", FieldTypes.ofInt())
			.withMeasure("eventCount", count(ofLong()))
			.withMeasure("sumRevenue", sum(ofDouble()))
			.withMeasure("minRevenue", min(ofDouble()))
			.withMeasure("maxRevenue", max(ofDouble()))
			.withMeasure("uniqueUserIds", union(ofLong()))
			.withMeasure("estimatedUniqueUserIdCount", hyperLogLog(1024))
			.build();
		AggregationState state = createAggregationState(structure);

		AggregationExecutor aggregationExecutor = new AggregationExecutor(reactor, executor, classLoader, aggregationChunkStorage, frameFormat, structure);
		aggregationExecutor.setTemporarySortDir(temporaryFolder.newFolder().toPath());

		StreamSupplier<EventRecord> supplier = StreamSuppliers.ofValues(
			new EventRecord(1, 0.34, 1),
			new EventRecord(2, 0.42, 3),
			new EventRecord(3, 0.13, 20));

		ProtoAggregationDiff diff = await(supplier.streamTo(aggregationExecutor.consume(EventRecord.class)));
		Set<String> protoChunkIds = diff.addedChunks().stream()
			.map(ProtoAggregationChunk::protoChunkId)
			.collect(toSet());
		Map<String, Long> chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		state.apply(materializeProtoDiff(diff, chunkIds));

		supplier = StreamSuppliers.ofValues(
			new EventRecord(2, 0.30, 20),
			new EventRecord(1, 0.22, 1000),
			new EventRecord(2, 0.91, 33));

		diff = await(supplier.streamTo(aggregationExecutor.consume(EventRecord.class)));
		protoChunkIds = diff.addedChunks().stream()
			.map(ProtoAggregationChunk::protoChunkId)
			.collect(toSet());
		chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		state.apply(materializeProtoDiff(diff, chunkIds));

		supplier = StreamSuppliers.ofValues(
			new EventRecord(1, 0.01, 1),
			new EventRecord(3, 0.88, 20),
			new EventRecord(3, 1.01, 21));

		diff = await(supplier.streamTo(aggregationExecutor.consume(EventRecord.class)));
		protoChunkIds = diff.addedChunks().stream()
			.map(ProtoAggregationChunk::protoChunkId)
			.collect(toSet());
		chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		state.apply(materializeProtoDiff(diff, chunkIds));

		supplier = StreamSuppliers.ofValues(
			new EventRecord(1, 0.35, 500),
			new EventRecord(1, 0.59, 17),
			new EventRecord(2, 0.85, 50));

		diff = await(supplier.streamTo(aggregationExecutor.consume(EventRecord.class)));
		protoChunkIds = diff.addedChunks().stream()
			.map(ProtoAggregationChunk::protoChunkId)
			.collect(toSet());
		chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		state.apply(materializeProtoDiff(diff, chunkIds));

		AggregationQuery query = new AggregationQuery();
		query.addKeys(List.of("siteId"));
		query.addMeasures(List.of("eventCount", "sumRevenue", "minRevenue", "maxRevenue", "uniqueUserIds", "estimatedUniqueUserIdCount"));

		List<AggregationChunk> chunks = state.findChunks(query.getMeasures(), query.getPredicate());
		List<QueryResult> queryResults = await(aggregationExecutor.query("", chunks, query, QueryResult.class, DefiningClassLoader.create(classLoader))
			.toList());

		double delta = 1E-3;
		assertEquals(3, queryResults.size());

		QueryResult s1 = queryResults.get(0);
		assertEquals(1, s1.siteId);
		assertEquals(5, s1.eventCount);
		assertEquals(1.51, s1.sumRevenue, delta);
		assertEquals(0.01, s1.minRevenue, delta);
		assertEquals(0.59, s1.maxRevenue, delta);
		assertEquals(Set.of(1L, 17L, 500L, 1000L), s1.uniqueUserIds);
		assertEquals(4, s1.estimatedUniqueUserIdCount.estimate());

		QueryResult s2 = queryResults.get(1);
		assertEquals(2, s2.siteId);
		assertEquals(4, s2.eventCount);
		assertEquals(2.48, s2.sumRevenue, delta);
		assertEquals(0.30, s2.minRevenue, delta);
		assertEquals(0.91, s2.maxRevenue, delta);
		assertEquals(Set.of(3L, 20L, 33L, 50L), s2.uniqueUserIds);
		assertEquals(4, s2.estimatedUniqueUserIdCount.estimate());

		QueryResult s3 = queryResults.get(2);
		assertEquals(3, s3.siteId);
		assertEquals(3, s3.eventCount);
		assertEquals(2.02, s3.sumRevenue, delta);
		assertEquals(0.13, s3.minRevenue, delta);
		assertEquals(1.01, s3.maxRevenue, delta);
		assertEquals(Set.of(20L, 21L), s3.uniqueUserIds);
		assertEquals(2, s3.estimatedUniqueUserIdCount.estimate());
	}

}
