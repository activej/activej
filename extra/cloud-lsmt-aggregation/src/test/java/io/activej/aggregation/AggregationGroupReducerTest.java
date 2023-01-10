package io.activej.aggregation;

import io.activej.aggregation.fieldtype.FieldTypes;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.codegen.DefiningClassLoader;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.aggregation.StreamUtils.assertEndOfStream;
import static io.activej.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.aggregation.measure.Measures.union;
import static io.activej.aggregation.util.Utils.*;
import static io.activej.common.Utils.keysToMap;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

@SuppressWarnings({"unchecked", "rawtypes"})
public class AggregationGroupReducerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Test
	public void test() {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong())
				.withKey("word", FieldTypes.ofString())
				.withMeasure("documents", union(ofInt()));

		List<StreamConsumer> listConsumers = new ArrayList<>();
		List items = new ArrayList();
		AsyncAggregationChunkStorage<Long> aggregationChunkStorage = new AsyncAggregationChunkStorage<>() {
			long chunkId;

			@Override
			public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				return Promise.of(StreamSupplier.ofIterable(items));
			}

			@Override
			public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				StreamConsumerToList consumer = StreamConsumerToList.create(items);
				listConsumers.add(consumer);
				return Promise.of(consumer);
			}

			@Override
			public Promise<Long> createId() {
				return Promise.of(++chunkId);
			}

			@Override
			public Promise<Void> finish(Set<Long> chunkIds) {
				return Promise.complete();
			}
		};

		Class<InvertedIndexRecord> inputClass = InvertedIndexRecord.class;
		Class<Comparable> keyClass = createKeyClass(
				keysToMap(Stream.of("word"), structure.getKeyTypes()::get),
				classLoader);
		Class<InvertedIndexRecord> aggregationClass = createRecordClass(structure, List.of("word"), List.of("documents"), classLoader);

		Function<InvertedIndexRecord, Comparable> keyFunction = createKeyFunction(inputClass, keyClass,
				List.of("word"), classLoader);

		Aggregate<InvertedIndexRecord, Object> aggregate = createPreaggregator(structure, inputClass, aggregationClass,
				Map.of("word", "word"), Map.of("documents", "documentId"), classLoader);

		int aggregationChunkSize = 2;

		StreamSupplier<InvertedIndexRecord> supplier = StreamSupplier.of(
				new InvertedIndexRecord("fox", 1),
				new InvertedIndexRecord("brown", 2),
				new InvertedIndexRecord("fox", 3),
				new InvertedIndexRecord("brown", 3),
				new InvertedIndexRecord("lazy", 4),
				new InvertedIndexRecord("dog", 1),
				new InvertedIndexRecord("quick", 1),
				new InvertedIndexRecord("fox", 4),
				new InvertedIndexRecord("brown", 10));

		AggregationGroupReducer<Long, InvertedIndexRecord, Comparable> groupReducer = new AggregationGroupReducer<>(aggregationChunkStorage,
				structure, List.of("documents"),
				aggregationClass, singlePartition(), keyFunction, aggregate, aggregationChunkSize, classLoader);

		await(supplier.streamTo(groupReducer));
		List<AggregationChunk> list = await(groupReducer.getResult());

		assertEndOfStream(supplier);
		assertEndOfStream(groupReducer);
		assertEquals(5, list.size());

		for (StreamConsumer consumer : listConsumers) {
			assertEndOfStream(consumer);
		}
	}

}
