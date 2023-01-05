package io.activej.aggregation;

import io.activej.aggregation.ot.AggregationStructure;
import io.activej.aggregation.util.PartitionPredicate;
import io.activej.codegen.DefiningClassLoader;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.activej.aggregation.StreamUtils.assertClosedWithError;
import static io.activej.aggregation.StreamUtils.assertEndOfStream;
import static io.activej.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.aggregation.util.Utils.createRecordClass;
import static io.activej.aggregation.util.Utils.singlePartition;
import static io.activej.common.Utils.last;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.assertCompleteFn;
import static org.junit.Assert.*;

@SuppressWarnings({"unchecked", "rawtypes"})
public final class AggregationChunkerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private final DefiningClassLoader classLoader = DefiningClassLoader.create();
	private final AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong())
			.withKey("key", ofInt())
			.withMeasure("value", sum(ofInt()))
			.withMeasure("timestamp", sum(ofLong()));
	private final AggregationState state = new AggregationState(structure);

	@Test
	public void test() {
		List<KeyValuePair> items = new ArrayList<>();
		AsyncAggregationChunkStorage<Long> aggregationChunkStorage = new AsyncAggregationChunkStorage<>() {
			long chunkId;

			@Override
			public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				return Promise.of(StreamSupplier.ofIterable((Iterable<T>) items));
			}

			@Override
			public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				StreamConsumerToList<T> consumer = StreamConsumerToList.create((List<T>) items);
				consumer.getAcknowledgement().whenComplete(assertCompleteFn());
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

		List<AggregationChunk> chunksToConsolidate = state.findChunksGroupWithMostOverlaps();

		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getMeasures()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		Class<?> recordClass = createRecordClass(structure, structure.getKeys(), fields, classLoader);

		AggregationChunker<?, KeyValuePair> chunker = AggregationChunker.create(
				structure, structure.getMeasures(), recordClass, (PartitionPredicate) singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		StreamSupplier<KeyValuePair> supplier = StreamSupplier.of(
				new KeyValuePair(3, 4, 6),
				new KeyValuePair(3, 6, 7),
				new KeyValuePair(1, 2, 1)
		);

		await(supplier.streamTo(chunker));
		List<AggregationChunk> list = await(chunker.getResult());

		assertEquals(3, list.size());
		assertEquals(new KeyValuePair(3, 4, 6), items.get(0));
		assertEquals(new KeyValuePair(3, 6, 7), items.get(1));
		assertEquals(new KeyValuePair(1, 2, 1), items.get(2));
	}

	@Test
	public void testSupplierWithError() {
		List<StreamConsumer> listConsumers = new ArrayList<>();
		AsyncAggregationChunkStorage<Long> aggregationChunkStorage = new AsyncAggregationChunkStorage<>() {
			long chunkId;
			final List items = new ArrayList();

			@Override
			public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				return Promise.of(StreamSupplier.ofIterable(items));
			}

			@Override
			public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				StreamConsumerToList<T> consumer = StreamConsumerToList.create(items);
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

		List<AggregationChunk> chunksToConsolidate = state.findChunksGroupWithMostOverlaps();

		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getMeasures()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		Class<?> recordClass = createRecordClass(structure, structure.getKeys(), fields, classLoader);

		ExpectedException exception = new ExpectedException("Test Exception");
		StreamSupplier<KeyValuePair> supplier = StreamSupplier.concat(
				StreamSupplier.of(
						new KeyValuePair(1, 1, 0),
						new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2)),
				StreamSupplier.of(
						new KeyValuePair(1, 1, 0),
						new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2)),
				StreamSupplier.of(
						new KeyValuePair(1, 1, 0),
						new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2)),
				StreamSupplier.of(
						new KeyValuePair(1, 1, 0),
						new KeyValuePair(1, 2, 1),
						new KeyValuePair(1, 1, 2)),
				StreamSupplier.closingWithError(exception)
		);
		AggregationChunker chunker = AggregationChunker.create(
				structure, structure.getMeasures(), recordClass, singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		Exception e = awaitException(supplier.streamTo(chunker));
		assertSame(exception, e);

		// this must be checked after eventloop completion :(
		for (int i = 0; i < listConsumers.size() - 1; i++) {
			assertEndOfStream(listConsumers.get(i));
		}
		assertClosedWithError(last(listConsumers));
	}

	@Test
	public void testStorageConsumerWithError() {
		List<StreamConsumer> listConsumers = new ArrayList<>();
		AsyncAggregationChunkStorage<Long> aggregationChunkStorage = new AsyncAggregationChunkStorage<>() {
			long chunkId;
			final List items = new ArrayList();

			@Override
			public <T> Promise<StreamSupplier<T>> read(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				return Promise.of(StreamSupplier.ofIterable(items));
			}

			@Override
			public <T> Promise<StreamConsumer<T>> write(AggregationStructure aggregation, List<String> fields, Class<T> recordClass, Long chunkId, DefiningClassLoader classLoader) {
				if (chunkId == 1) {
					StreamConsumerToList<T> toList = StreamConsumerToList.create(items);
					listConsumers.add(toList);
					return Promise.of(toList);
				} else {
					StreamConsumer<T> consumer = StreamConsumer.closingWithError(new Exception("Test Exception"));
					listConsumers.add(consumer);
					return Promise.of(consumer);
				}
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

		List<AggregationChunk> chunksToConsolidate = state.findChunksGroupWithMostOverlaps();

		List<String> fields = new ArrayList<>();
		for (AggregationChunk chunk : chunksToConsolidate) {
			for (String field : chunk.getMeasures()) {
				if (!fields.contains(field)) {
					fields.add(field);
				}
			}
		}

		Class<?> recordClass = createRecordClass(structure, structure.getKeys(), fields, classLoader);

		StreamSupplier<KeyValuePair> supplier = StreamSupplier.of(
				new KeyValuePair(1, 1, 0),
				new KeyValuePair(1, 2, 1),
				new KeyValuePair(1, 1, 2),
				new KeyValuePair(1, 1, 2),
				new KeyValuePair(1, 1, 2));
		AggregationChunker chunker = AggregationChunker.create(
				structure, structure.getMeasures(), recordClass, singlePartition(),
				aggregationChunkStorage, classLoader, 1);

		awaitException(supplier.streamTo(chunker));

		assertTrue(listConsumers.size() > 1);
		for (int i = 0; i < listConsumers.size() - 2; i++) {
			assertEndOfStream(listConsumers.get(i));
		}
		assertClosedWithError(listConsumers.get(listConsumers.size() - 2));
		assertClosedWithError(listConsumers.get(listConsumers.size() - 1));
	}
}
