package io.activej.cube;

import io.activej.aggregation.AggregationChunkStorage;
import io.activej.aggregation.AggregationPredicates;
import io.activej.aggregation.AsyncAggregationChunkStorage;
import io.activej.aggregation.ChunkIdCodec;
import io.activej.async.function.AsyncSupplier;
import io.activej.common.ref.RefLong;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.cube.bean.TestPubRequest;
import io.activej.cube.bean.TestPubRequest.TestAdvRequest;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamSupplier;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOTProcessor;
import io.activej.etl.LogOTState;
import io.activej.fs.LocalFs;
import io.activej.multilog.AsyncMultilog;
import io.activej.multilog.Multilog;
import io.activej.ot.OTStateManager;
import io.activej.ot.uplink.AsyncOTUplink;
import io.activej.serializer.SerializerBuilder;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;

import static io.activej.aggregation.AggregationPredicates.alwaysTrue;
import static io.activej.aggregation.fieldtype.FieldTypes.*;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.cube.Cube.AggregationConfig.id;
import static io.activej.cube.TestUtils.runProcessLogs;
import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public final class LogToCubeTest extends CubeTestBase {

	@Test
	public void testStubStorage() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		LocalFs fs = LocalFs.create(reactor, EXECUTOR, aggregationsDir);
		await(fs.start());
		FrameFormat frameFormat = LZ4FrameFormat.create();
		AsyncAggregationChunkStorage<Long> aggregationChunkStorage = AggregationChunkStorage.create(reactor, ChunkIdCodec.ofLong(), AsyncSupplier.of(new RefLong(0)::inc), frameFormat, fs);
		Cube cube = Cube.create(reactor, EXECUTOR, CLASS_LOADER, aggregationChunkStorage)
				.withDimension("pub", ofInt())
				.withDimension("adv", ofInt())
				.withDimension("testEnum", ofEnum(TestPubRequest.TestEnum.class))
				.withMeasure("pubRequests", sum(ofLong()))
				.withMeasure("advRequests", sum(ofLong()))
				.withAggregation(id("pub").withDimensions("pub", "testEnum").withMeasures("pubRequests")
//						.withPredicate(AggregationPredicates.notEq("testEnum", null)) // ok
								.withPredicate(AggregationPredicates.has("testEnum")) // fail
				)
				.withAggregation(id("adv").withDimensions("adv").withMeasures("advRequests"));

		AsyncOTUplink<Long, LogDiff<CubeDiff>, ?> uplink = uplinkFactory.create(cube);

		List<TestAdvResult> expected = List.of(new TestAdvResult(10, 2), new TestAdvResult(20, 1), new TestAdvResult(30, 1));

		LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube);
		OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager = OTStateManager.create(reactor, LOG_OT, uplink, cubeDiffLogOTState);

		LocalFs localFs = LocalFs.create(reactor, EXECUTOR, logsDir);
		await(localFs.start());
		AsyncMultilog<TestPubRequest> multilog = Multilog.create(reactor, localFs,
				frameFormat,
				SerializerBuilder.create(CLASS_LOADER).build(TestPubRequest.class),
				NAME_PARTITION_REMAINDER_SEQ);

		LogOTProcessor<TestPubRequest, CubeDiff> logOTProcessor = LogOTProcessor.create(reactor,
				multilog,
				new TestAggregatorSplitter(cube), // TestAggregatorSplitter.create(EVENTLOOP, cube),
				"testlog",
				List.of("partitionA"),
				cubeDiffLogOTState);

		StreamSupplier<TestPubRequest> supplier = StreamSupplier.of(
				new TestPubRequest(1000, 1, List.of(new TestAdvRequest(10))),
				new TestPubRequest(1001, 2, List.of(new TestAdvRequest(10), new TestAdvRequest(20))),
				new TestPubRequest(1002, 1, List.of(new TestAdvRequest(30))),
				new TestPubRequest(1002, 2, List.of()));

		await(supplier.streamTo(StreamConsumer.ofPromise(multilog.write("partitionA"))));
		await(logCubeStateManager.checkout());
		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);

		List<TestAdvResult> list = await(cube.queryRawStream(
						List.of("adv"),
						List.of("advRequests"),
						alwaysTrue(),
						TestAdvResult.class, CLASS_LOADER)
				.toList());

		assertEquals(expected, list);
	}

	public static final class TestAdvResult {
		public int adv;
		public long advRequests;

		@SuppressWarnings("unused")
		public TestAdvResult() {
		}

		public TestAdvResult(int adv, long advRequests) {
			this.adv = adv;
			this.advRequests = advRequests;
		}

		@Override
		public String toString() {
			return "TestAdvResult{adv=" + adv + ", advRequests=" + advRequests + '}';
		}

		@SuppressWarnings("RedundantIfStatement")
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestAdvResult that = (TestAdvResult) o;

			if (adv != that.adv) return false;
			if (advRequests != that.advRequests) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = adv;
			result = 31 * result + (int) (advRequests ^ (advRequests >>> 32));
			return result;
		}
	}
}
