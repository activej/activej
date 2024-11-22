package io.activej.cube.aggregation;

import io.activej.codegen.DefiningClassLoader;
import io.activej.cube.AggregationStructure;
import io.activej.cube.aggregation.fieldtype.FieldTypes;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.activej.common.collection.CollectorUtils.toLinkedHashMap;
import static io.activej.cube.TestUtils.aggregationStructureBuilder;
import static io.activej.cube.aggregation.StreamUtils.assertEndOfStream;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.cube.aggregation.measure.Measures.union;
import static io.activej.cube.aggregation.util.Utils.*;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
		AggregationStructure structure = aggregationStructureBuilder()
			.withKey("word", FieldTypes.ofString())
			.withMeasure("documents", union(ofInt()))
			.build();

		List<StreamConsumer> listConsumers = new ArrayList<>();
		List items = new ArrayList();
		IAggregationChunkStorage aggregationChunkStorage = new IAggregationChunkStorage() {
			long chunkId;
			final Map<String, Long> idsMap = new HashMap<>();

			@Override
			public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, long chunkId, DefiningClassLoader classLoader) {
				return Promise.of(StreamSuppliers.ofIterable(items));
			}

			@Override
			public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, String protoChunkId, DefiningClassLoader classLoader) {
				StreamConsumer<T> consumer = ToListStreamConsumer.create(items);
				listConsumers.add(consumer);
				return Promise.of(consumer);
			}

			@Override
			public Promise<String> createProtoChunkId() {
				String protoId = UUID.randomUUID().toString();
				idsMap.put(protoId, ++chunkId);
				return Promise.of(protoId);
			}

			@Override
			public Promise<Map<String, Long>> finish(Set<String> protoChunkIds) {
				return Promise.of(protoChunkIds.stream().collect(Collectors.toMap(Function.identity(), idsMap::get)));
			}

			@Override
			public Promise<Set<Long>> listChunks() {
				return Promise.of(Set.of());
			}

			@Override
			public Promise<Void> deleteChunks(Set<Long> chunksToDelete) {
				return Promise.complete();
			}
		};

		Class<InvertedIndexRecord> inputClass = InvertedIndexRecord.class;
		Class<Comparable> keyClass = createKeyClass(
			Stream.of("word")
				.collect(toLinkedHashMap(structure.getKeyTypes()::get)),
			classLoader);
		Class<InvertedIndexRecord> aggregationClass = createRecordClass(structure, List.of("word"), List.of("documents"), classLoader);

		Function<InvertedIndexRecord, Comparable> keyFunction = createKeyFunction(inputClass, keyClass,
			List.of("word"), classLoader);

		Aggregate<InvertedIndexRecord, Object> aggregate = createPreaggregator(structure, inputClass, aggregationClass,
			Map.of("word", "word"), Map.of("documents", "documentId"), classLoader);

		int aggregationChunkSize = 2;

		StreamSupplier<InvertedIndexRecord> supplier = StreamSuppliers.ofValues(
			new InvertedIndexRecord("fox", 1),
			new InvertedIndexRecord("brown", 2),
			new InvertedIndexRecord("fox", 3),
			new InvertedIndexRecord("brown", 3),
			new InvertedIndexRecord("lazy", 4),
			new InvertedIndexRecord("dog", 1),
			new InvertedIndexRecord("quick", 1),
			new InvertedIndexRecord("fox", 4),
			new InvertedIndexRecord("brown", 10));

		AggregationGroupReducer<InvertedIndexRecord, Comparable> groupReducer = new AggregationGroupReducer<>(aggregationChunkStorage,
			structure, List.of("documents"),
			aggregationClass, singlePartition(), keyFunction, aggregate, aggregationChunkSize, classLoader);

		await(supplier.streamTo(groupReducer));
		List<ProtoAggregationChunk> list = await(groupReducer.getResult());

		assertEndOfStream(supplier);
		assertEndOfStream(groupReducer);
		assertEquals(4, list.size());
		for (ProtoAggregationChunk protoAggregationChunk : list) {
			assertTrue(protoAggregationChunk.count() <= aggregationChunkSize);
		}

		for (StreamConsumer consumer : listConsumers) {
			assertEndOfStream(consumer);
		}
	}

}
