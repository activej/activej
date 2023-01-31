package io.activej.aggregation;

import io.activej.aggregation.ot.AggregationStructure;
import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.ref.RefLong;
import io.activej.csp.process.frames.LZ4FrameFormat;
import io.activej.datastream.StreamSupplier;
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

import static io.activej.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.aggregation.util.Utils.singlePartition;
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
	private final AggregationStructure structure = AggregationStructure.builder(ChunkIdJsonCodec.ofLong())
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
		IAggregationChunkStorage<Long> aggregationChunkStorage = AggregationChunkStorage.create(
				reactor,
				ChunkIdJsonCodec.ofLong(),
				AsyncSupplier.of(new RefLong(0)::inc),
				LZ4FrameFormat.create(),
				fs);

		int nChunks = 100;
		AggregationChunker<?, KeyValuePair> chunker = AggregationChunker.create(
				structure, structure.getMeasures(), KeyValuePair.class, singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		Set<Path> expected = IntStream.range(0, nChunks + 1).mapToObj(i -> Paths.get((i + 1) + ".temp")).collect(toSet());

		Random random = ThreadLocalRandom.current();
		StreamSupplier<KeyValuePair> supplier = StreamSupplier.ofStream(
				Stream.generate(() -> new KeyValuePair(random.nextInt(), random.nextInt(), random.nextLong()))
						.limit(nChunks));

		List<Path> paths = await(supplier.streamTo(chunker)
				.map($ -> {
					try (Stream<Path> list = Files.list(storageDir)) {
						return list.collect(toList());
					} catch (IOException e) {
						throw new AssertionError(e);
					}
				}));

		Set<Path> actual = paths.stream().filter(Files::isRegularFile).map(Path::getFileName).collect(toSet());

		assertEquals(expected, actual);
	}
}
