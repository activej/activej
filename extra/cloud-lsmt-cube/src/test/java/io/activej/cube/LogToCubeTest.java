package io.activej.cube;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.ref.RefLong;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.aggregation.AggregationChunkStorage;
import io.activej.cube.aggregation.ChunkIdJsonCodec;
import io.activej.cube.aggregation.IAggregationChunkStorage;
import io.activej.cube.bean.TestPubRequest;
import io.activej.cube.bean.TestPubRequest.TestAdvRequest;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.etl.LogDiff;
import io.activej.etl.LogProcessor;
import io.activej.etl.LogState;
import io.activej.etl.StateQueryFunction;
import io.activej.fs.FileSystem;
import io.activej.multilog.IMultilog;
import io.activej.multilog.Multilog;
import io.activej.ot.OTStateManager;
import io.activej.ot.uplink.AsyncOTUplink;
import io.activej.serializer.SerializerFactory;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;

import static io.activej.cube.CubeStructure.AggregationConfig.id;
import static io.activej.cube.TestUtils.runProcessLogs;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.*;
import static io.activej.cube.aggregation.measure.Measures.sum;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.*;
import static io.activej.etl.StateQueryFunction.ofState;
import static io.activej.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public final class LogToCubeTest extends CubeTestBase {

	@Test
	public void testStubStorage() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		FileSystem fs = FileSystem.create(reactor, EXECUTOR, aggregationsDir);
		await(fs.start());
		FrameFormat frameFormat = FrameFormats.lz4();
		IAggregationChunkStorage<Long> aggregationChunkStorage = AggregationChunkStorage.create(reactor, ChunkIdJsonCodec.ofLong(), AsyncSupplier.of(new RefLong(0)::inc), frameFormat, fs);
		CubeStructure cubeStructure = CubeStructure.builder()
			.withDimension("pub", ofInt())
			.withDimension("adv", ofInt())
			.withDimension("testEnum", ofEnum(TestPubRequest.TestEnum.class), notEq("testEnum", null))
			.withMeasure("pubRequests", sum(ofLong()))
			.withMeasure("advRequests", sum(ofLong()))
			.withAggregation(id("pub")
				.withDimensions("pub", "testEnum")
				.withMeasures("pubRequests")
				.withPredicate(has("testEnum"))
			)
			.withAggregation(id("adv")
				.withDimensions("adv")
				.withMeasures("advRequests"))
			.build();

		AsyncOTUplink<Long, LogDiff<CubeDiff>, ?> uplink = uplinkFactory.create(cubeStructure, description);

		List<TestAdvResult> expected = List.of(new TestAdvResult(10, 2), new TestAdvResult(20, 1), new TestAdvResult(30, 1));

		CubeState cubeState = CubeState.create(cubeStructure);
		LogState<CubeDiff, CubeState> cubeDiffLogState = LogState.create(cubeState);
		OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager = OTStateManager.create(reactor, LOG_OT, uplink, cubeDiffLogState);

		FileSystem fileSystem = FileSystem.create(reactor, EXECUTOR, logsDir);
		await(fileSystem.start());
		IMultilog<TestPubRequest> multilog = Multilog.create(reactor, fileSystem,
			frameFormat,
			SerializerFactory.defaultInstance().create(CLASS_LOADER, TestPubRequest.class),
			NAME_PARTITION_REMAINDER_SEQ);

		CubeExecutor cubeExecutor = CubeExecutor.builder(reactor, cubeStructure, EXECUTOR, CLASS_LOADER, aggregationChunkStorage).build();

		LogProcessor<TestPubRequest, CubeDiff> logProcessor = LogProcessor.create(reactor,
			multilog,
			new TestAggregatorSplitter(cubeExecutor), // TestAggregatorSplitter.create(EVENTLOOP, cube),
			"testlog",
			List.of("partitionA"),
			ofState(cubeDiffLogState));

		StreamSupplier<TestPubRequest> supplier = StreamSuppliers.ofValues(
			new TestPubRequest(1000, 1, List.of(new TestAdvRequest(10))),
			new TestPubRequest(1001, 2, List.of(new TestAdvRequest(10), new TestAdvRequest(20))),
			new TestPubRequest(1002, 1, List.of(new TestAdvRequest(30))),
			new TestPubRequest(1002, 2, List.of()));

		await(supplier.streamTo(StreamConsumers.ofPromise(multilog.write("partitionA"))));
		await(logCubeStateManager.checkout());
		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logProcessor);

		StateQueryFunction<CubeState> stateFunction = ofState(cubeState);
		CubeReporting cubeReporting = CubeReporting.create(stateFunction, cubeStructure, cubeExecutor);

		List<TestAdvResult> list = await(cubeReporting.queryRawStream(
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
