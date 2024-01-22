package io.activej.cube;

import io.activej.codegen.DefiningClassLoader;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.aggregation.AggregationChunkStorage;
import io.activej.cube.aggregation.IAggregationChunkStorage;
import io.activej.cube.bean.DataItemResultString;
import io.activej.cube.bean.DataItemString1;
import io.activej.cube.bean.DataItemString2;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.ProtoCubeDiff;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.etl.LogDiff;
import io.activej.etl.LogState;
import io.activej.fs.FileSystem;
import io.activej.ot.StateManager;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.cube.CubeStructure.AggregationConfig.id;
import static io.activej.cube.TestUtils.stubChunkIdGenerator;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.*;
import static io.activej.cube.aggregation.measure.Measures.sum;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.and;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.eq;
import static io.activej.cube.aggregation.util.Utils.materializeProtoCubeDiff;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class StringDimensionTest extends CubeTestBase {

	@Test
	public void testQuery() throws Exception {
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Executor executor = Executors.newCachedThreadPool();

		FileSystem fs = FileSystem.create(reactor, executor, aggregationsDir);
		await(fs.start());
		IAggregationChunkStorage aggregationChunkStorage = AggregationChunkStorage.create(reactor,
			stubChunkIdGenerator(), FrameFormats.lz4(), fs);
		CubeStructure cubeStructure = CubeStructure.builder()
			.withDimension("key1", ofString())
			.withDimension("key2", ofInt())
			.withMeasure("metric1", sum(ofLong()))
			.withMeasure("metric2", sum(ofLong()))
			.withMeasure("metric3", sum(ofLong()))
			.withAggregation(id("detailedAggregation")
				.withDimensions("key1", "key2")
				.withMeasures("metric1", "metric2", "metric3"))
			.build();

		CubeExecutor cubeExecutor = CubeExecutor.create(reactor, cubeStructure, executor, CLASS_LOADER, aggregationChunkStorage);

		StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager = stateManagerFactory.create(cubeStructure, description);
		CubeReporting cubeReporting = CubeReporting.create(stateManager, cubeStructure, cubeExecutor);

		ProtoCubeDiff consumer1Result = await(StreamSuppliers.ofValues(
				new DataItemString1("str1", 2, 10, 20),
				new DataItemString1("str2", 3, 10, 20))
			.streamTo(cubeExecutor.consume(DataItemString1.class)));

		ProtoCubeDiff consumer2Result = await(StreamSuppliers.ofValues(
				new DataItemString2("str2", 3, 10, 20),
				new DataItemString2("str1", 4, 10, 20))
			.streamTo(cubeExecutor.consume(DataItemString2.class)));

		List<String> protoChunkIds1 = consumer1Result.addedProtoChunks().toList();
		List<Long> chunkIds1 = await(aggregationChunkStorage.finish(protoChunkIds1));
		List<String> protoChunkIds2 = consumer2Result.addedProtoChunks().toList();
		List<Long> chunkIds2 = await(aggregationChunkStorage.finish(protoChunkIds2));

		await(stateManager.push(List.of(LogDiff.forCurrentPosition(List.of(
			materializeProtoCubeDiff(consumer1Result, protoChunkIds1, chunkIds1),
			materializeProtoCubeDiff(consumer2Result, protoChunkIds2, chunkIds2)
		)))));

		ToListStreamConsumer<DataItemResultString> consumerToList = ToListStreamConsumer.create();
		await(cubeReporting.queryRawStream(List.of("key1", "key2"), List.of("metric1", "metric2", "metric3"),
				and(eq("key1", "str2"), eq("key2", 3)),
				DataItemResultString.class, DefiningClassLoader.create(CLASS_LOADER))
			.streamTo(consumerToList));

		List<DataItemResultString> actual = consumerToList.getList();
		List<DataItemResultString> expected = List.of(new DataItemResultString("str2", 3, 10, 30, 20));

		assertEquals(expected, actual);
	}
}
