package io.activej.cube.aggregation;

import io.activej.codegen.DefiningClassLoader;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.AggregationStructure;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.activej.cube.TestUtils.aggregationStructureBuilder;
import static io.activej.cube.TestUtils.stubChunkIdGenerator;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.cube.aggregation.measure.Measures.sum;
import static io.activej.cube.aggregation.util.Utils.singlePartition;
import static io.activej.promise.TestUtils.await;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class AggregationChunkStorageTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private final DefiningClassLoader classLoader = DefiningClassLoader.create();
	private final AggregationStructure structure = aggregationStructureBuilder()
		.withKey("key", ofInt())
		.withMeasure("value", sum(ofInt()))
		.withMeasure("timestamp", sum(ofLong()))
		.build();

	@Test
	public void testAcknowledge() throws IOException {
		Reactor reactor = Reactor.getCurrentReactor();
		Path storageDir = temporaryFolder.newFolder().toPath();
		FileSystem fs = FileSystem.create(reactor, newCachedThreadPool(), storageDir);
		await(fs.start());
		IAggregationChunkStorage aggregationChunkStorage = AggregationChunkStorage.create(
			reactor,
			stubChunkIdGenerator(),
			FrameFormats.lz4(),
			fs);

		int nChunks = 100;
		AggregationChunker<KeyValuePair> chunker = AggregationChunker.create(
			structure, structure.getMeasures(), KeyValuePair.class, singlePartition(),
			aggregationChunkStorage, classLoader, 1);

		Set<Path> expected = IntStream.range(0, nChunks).mapToObj(i -> Paths.get((i + 1) + AggregationChunkStorage.LOG)).collect(toSet());

		Random random = ThreadLocalRandom.current();
		StreamSupplier<KeyValuePair> supplier = StreamSuppliers.ofStream(
			Stream.generate(() -> new KeyValuePair(random.nextInt(), random.nextInt(), random.nextLong()))
				.limit(nChunks));

		List<Path> paths = await(supplier.streamTo(chunker)
			.then(chunker::getResult)
			.then(protoAggregationChunks -> aggregationChunkStorage.finish(protoAggregationChunks.stream()
				.map(ProtoAggregationChunk::protoChunkId)
				.collect(toSet())))
			.map($ -> {
				try (Stream<Path> list = Files.list(storageDir)) {
					return list.filter(path -> path.toString().endsWith(AggregationChunkStorage.LOG)).collect(toList());
				} catch (IOException e) {
					throw new AssertionError(e);
				}
			}));

		Set<Path> actual = paths.stream().filter(Files::isRegularFile).map(Path::getFileName).collect(toSet());

		assertEquals(expected, actual);
	}
}
