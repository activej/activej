package io.activej.cube;

import io.activej.codegen.DefiningClassLoader;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.aggregation.*;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.aggregation.ot.ProtoAggregationDiff;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.fs.FileSystem;
import io.activej.ot.OTState;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.cube.TestUtils.*;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofString;
import static io.activej.cube.aggregation.measure.Measures.union;
import static io.activej.cube.aggregation.util.Utils.materializeProtoAggregationDiff;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class InvertedIndexTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	public static class InvertedIndexQueryResult {
		public String word;
		public Set<Integer> documents;

		@SuppressWarnings("unused")
		public InvertedIndexQueryResult() {
		}

		public InvertedIndexQueryResult(String word, Set<Integer> documents) {
			this.word = word;
			this.documents = documents;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			InvertedIndexQueryResult that = (InvertedIndexQueryResult) o;

			if (!Objects.equals(word, that.word)) return false;
			return Objects.equals(documents, that.documents);

		}

		@Override
		public int hashCode() {
			int result = word != null ? word.hashCode() : 0;
			result = 31 * result + (documents != null ? documents.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return
				"InvertedIndexQueryResult{" +
				"word='" + word + '\'' +
				", documents=" + documents +
				'}';
		}
	}

	@Test
	public void testInvertedIndex() throws Exception {
		Executor executor = Executors.newCachedThreadPool();
		Reactor reactor = Reactor.getCurrentReactor();
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Path path = temporaryFolder.newFolder().toPath();
		FileSystem fs = FileSystem.create(reactor, executor, path);
		await(fs.start());
		FrameFormat frameFormat = FrameFormats.lz4();
		IAggregationChunkStorage aggregationChunkStorage = AggregationChunkStorage.create(reactor, stubChunkIdGenerator(), frameFormat, fs);

		AggregationStructure structure = aggregationStructureBuilder()
			.withKey("word", ofString())
			.withMeasure("documents", union(ofInt()))
			.build();
		AggregationState state = createAggregationState(structure);

		AggregationExecutor aggregationExecutor = new AggregationExecutor(reactor, executor, classLoader, aggregationChunkStorage, frameFormat, structure);
		aggregationExecutor.setTemporarySortDir(temporaryFolder.newFolder().toPath());

		StreamSupplier<InvertedIndexRecord> supplier = StreamSuppliers.ofValues(
			new InvertedIndexRecord("fox", 1),
			new InvertedIndexRecord("brown", 2),
			new InvertedIndexRecord("fox", 3));

		doProcess(state, aggregationChunkStorage, aggregationExecutor, supplier);

		supplier = StreamSuppliers.ofValues(
			new InvertedIndexRecord("brown", 3),
			new InvertedIndexRecord("lazy", 4),
			new InvertedIndexRecord("dog", 1));

		doProcess(state, aggregationChunkStorage, aggregationExecutor, supplier);

		supplier = StreamSuppliers.ofValues(
			new InvertedIndexRecord("quick", 1),
			new InvertedIndexRecord("fox", 4),
			new InvertedIndexRecord("brown", 10));

		doProcess(state, aggregationChunkStorage, aggregationExecutor, supplier);

		AggregationQuery query = new AggregationQuery();
		query.addKeys(List.of("word"));
		query.addMeasures(List.of("documents"));

		List<AggregationChunk> chunks = state.findChunks(query.getMeasures(), query.getPredicate());
		List<InvertedIndexQueryResult> list = await(aggregationExecutor.query(chunks, query, InvertedIndexQueryResult.class, DefiningClassLoader.create(classLoader))
			.toList());

		List<InvertedIndexQueryResult> expectedResult = List.of(
			new InvertedIndexQueryResult("brown", Set.of(2, 3, 10)),
			new InvertedIndexQueryResult("dog", Set.of(1)),
			new InvertedIndexQueryResult("fox", Set.of(1, 3, 4)),
			new InvertedIndexQueryResult("lazy", Set.of(4)),
			new InvertedIndexQueryResult("quick", Set.of(1)));

		assertEquals(expectedResult, list);
	}

	public void doProcess(OTState<AggregationDiff> state, IAggregationChunkStorage aggregationChunkStorage, AggregationExecutor aggregation, StreamSupplier<InvertedIndexRecord> supplier) {
		ProtoAggregationDiff diff = await(supplier.streamTo(aggregation.consume(InvertedIndexRecord.class)));
		List<String> protoChunkIds = getAddedChunks(diff);
		List<Long> chunkIds = await(aggregationChunkStorage.finish(protoChunkIds));
		state.apply(materializeProtoAggregationDiff(diff, protoChunkIds, chunkIds));
	}

	private List<String> getAddedChunks(ProtoAggregationDiff aggregationDiff) {
		return aggregationDiff.addedChunks().stream().map(ProtoAggregationChunk::protoChunkId).toList();
	}

}
