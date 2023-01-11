package io.activej.aggregation;

import io.activej.aggregation.annotation.Key;
import io.activej.aggregation.annotation.Measures;
import io.activej.aggregation.fieldtype.FieldTypes;
import io.activej.aggregation.measure.HyperLogLog;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.ref.RefLong;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.FrameFormat_LZ4;
import io.activej.datastream.StreamSupplier;
import io.activej.fs.Fs_Local;
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
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.activej.aggregation.fieldtype.FieldTypes.ofDouble;
import static io.activej.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.aggregation.measure.Measures.*;
import static io.activej.promise.TestUtils.await;
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
	public record EventRecord(@Key int siteId,
							  @Measures({"sumRevenue", "minRevenue", "maxRevenue"}) double revenue,
							  @Measures({"uniqueUserIds", "estimatedUniqueUserIdCount"}) long userId) {
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
			return "QueryResult{" +
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
		Fs_Local fs = Fs_Local.create(reactor, executor, path);
		await(fs.start());
		FrameFormat frameFormat = FrameFormat_LZ4.create();
		AsyncAggregationChunkStorage<Long> aggregationChunkStorage = AggregationChunkStorage_Reactive.create(reactor, JsonCodec_ChunkId.ofLong(), AsyncSupplier.of(new RefLong(0)::inc), frameFormat, fs);

		AggregationStructure structure = AggregationStructure.create(JsonCodec_ChunkId.ofLong())
				.withKey("siteId", FieldTypes.ofInt())
				.withMeasure("eventCount", count(ofLong()))
				.withMeasure("sumRevenue", sum(ofDouble()))
				.withMeasure("minRevenue", min(ofDouble()))
				.withMeasure("maxRevenue", max(ofDouble()))
				.withMeasure("uniqueUserIds", union(ofLong()))
				.withMeasure("estimatedUniqueUserIdCount", hyperLogLog(1024));

		Aggregation_Reactive aggregation = Aggregation_Reactive.create(reactor, executor, classLoader, aggregationChunkStorage, frameFormat, structure)
				.withTemporarySortDir(temporaryFolder.newFolder().toPath());

		StreamSupplier<EventRecord> supplier = StreamSupplier.of(
				new EventRecord(1, 0.34, 1),
				new EventRecord(2, 0.42, 3),
				new EventRecord(3, 0.13, 20));

		AggregationDiff diff = await(supplier.streamTo(aggregation.consume(EventRecord.class)));
		aggregationChunkStorage.finish(diff.getAddedChunks().stream().map(AggregationChunk::getChunkId).map(id -> (long) id).collect(Collectors.toSet()));
		aggregation.getState().apply(diff);

		supplier = StreamSupplier.of(
				new EventRecord(2, 0.30, 20),
				new EventRecord(1, 0.22, 1000),
				new EventRecord(2, 0.91, 33));

		diff = await(supplier.streamTo(aggregation.consume(EventRecord.class)));
		aggregationChunkStorage.finish(diff.getAddedChunks().stream().map(AggregationChunk::getChunkId).map(id -> (long) id).collect(Collectors.toSet()));
		aggregation.getState().apply(diff);

		supplier = StreamSupplier.of(
				new EventRecord(1, 0.01, 1),
				new EventRecord(3, 0.88, 20),
				new EventRecord(3, 1.01, 21));

		diff = await(supplier.streamTo(aggregation.consume(EventRecord.class)));
		aggregationChunkStorage.finish(diff.getAddedChunks().stream().map(AggregationChunk::getChunkId).map(id -> (long) id).collect(Collectors.toSet()));
		aggregation.getState().apply(diff);

		supplier = StreamSupplier.of(
				new EventRecord(1, 0.35, 500),
				new EventRecord(1, 0.59, 17),
				new EventRecord(2, 0.85, 50));

		diff = await(supplier.streamTo(aggregation.consume(EventRecord.class)));
		aggregationChunkStorage.finish(diff.getAddedChunks().stream().map(AggregationChunk::getChunkId).map(id -> (long) id).collect(Collectors.toSet()));
		aggregation.getState().apply(diff);

		AggregationQuery query = AggregationQuery.create()
				.withKeys("siteId")
				.withMeasures("eventCount", "sumRevenue", "minRevenue", "maxRevenue", "uniqueUserIds", "estimatedUniqueUserIdCount");

		List<QueryResult> queryResults = await(aggregation.query(query, QueryResult.class, DefiningClassLoader.create(classLoader))
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
