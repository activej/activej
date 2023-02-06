package io.activej.datastream.processor.reducer;

import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.processor.DataItem1;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Objects;

import static io.activej.datastream.TestStreamTransformers.*;
import static io.activej.datastream.TestUtils.*;
import static io.activej.datastream.processor.reducer.Reducers.deduplicateReducer;
import static io.activej.datastream.processor.reducer.Reducers.mergeReducer;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.function.Function.identity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamReducerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testEmpty() {
		StreamSupplier<Integer> source = StreamSuppliers.empty();

		StreamReducer<Integer, Integer, Void> streamReducer = StreamReducer.<Integer, Integer, Void>builder()
				.withBufferSize(1)
				.build();

		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		await(
				source.streamTo(streamReducer.newInput(identity(), deduplicateReducer())),
				streamReducer.getOutput()
						.streamTo(consumer
								.transformWith(randomlySuspending()))
		);

		assertEquals(List.of(), consumer.getList());
		assertEndOfStream(source);
		assertEndOfStream(streamReducer.getOutput());
		assertConsumersEndOfStream(streamReducer.getInputs());
	}

	@Test
	public void testDeduplicate2() {
		StreamSupplier<Integer> source0 = StreamSuppliers.empty();
		StreamSupplier<Integer> source1 = StreamSuppliers.ofValue(7);
		StreamSupplier<Integer> source2 = StreamSuppliers.ofValues(3, 4, 6);
		StreamSupplier<Integer> source3 = StreamSuppliers.empty();
		StreamSupplier<Integer> source4 = StreamSuppliers.ofValues(2, 3, 5);
		StreamSupplier<Integer> source5 = StreamSuppliers.ofValues(1, 3);
		StreamSupplier<Integer> source6 = StreamSuppliers.ofValues(1, 3);
		StreamSupplier<Integer> source7 = StreamSuppliers.empty();

		StreamReducer<Integer, Integer, Void> streamReducer = StreamReducer.<Integer, Integer, Void>builder()
				.withBufferSize(1)
				.build();

		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		await(
				source0.streamTo(streamReducer.newInput(identity(), deduplicateReducer())),
				source1.streamTo(streamReducer.newInput(identity(), deduplicateReducer())),
				source2.streamTo(streamReducer.newInput(identity(), deduplicateReducer())),
				source3.streamTo(streamReducer.newInput(identity(), deduplicateReducer())),
				source4.streamTo(streamReducer.newInput(identity(), deduplicateReducer())),
				source5.streamTo(streamReducer.newInput(identity(), deduplicateReducer())),
				source6.streamTo(streamReducer.newInput(identity(), deduplicateReducer())),
				source7.streamTo(streamReducer.newInput(identity(), deduplicateReducer())),

				streamReducer.getOutput()
						.streamTo(consumer.transformWith(randomlySuspending()))
		);

		assertEquals(List.of(1, 2, 3, 4, 5, 6, 7), consumer.getList());
		assertEndOfStream(source0);
		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(source3);
		assertEndOfStream(source4);
		assertEndOfStream(source5);
		assertEndOfStream(source6);
		assertEndOfStream(source7);

		assertEndOfStream(streamReducer.getOutput());
		assertConsumersEndOfStream(streamReducer.getInputs());
	}

	@Test
	public void testWithError() {
		StreamSupplier<KeyValue1> source1 = StreamSuppliers.ofValues(
				new KeyValue1(1, 10.0),
				new KeyValue1(3, 30.0));
		StreamSupplier<KeyValue2> source2 = StreamSuppliers.ofValues(
				new KeyValue2(1, 10.0),
				new KeyValue2(3, 30.0));
		StreamSupplier<KeyValue3> source3 = StreamSuppliers.ofValues(
				new KeyValue3(2, 10.0, 20.0),
				new KeyValue3(3, 10.0, 20.0));

		StreamReducer<Integer, KeyValueResult, KeyValueResult> streamReducer = StreamReducer.<Integer, KeyValueResult, KeyValueResult>builder()
				.withBufferSize(1)
				.build();

		ToListStreamConsumer<KeyValueResult> consumer = ToListStreamConsumer.create();
		ExpectedException exception = new ExpectedException("Test Exception");

		Exception e = awaitException(
				source1.streamTo(streamReducer.newInput(input -> input.key, KeyValue1.REDUCER)),
				source2.streamTo(streamReducer.newInput(input -> input.key, KeyValue2.REDUCER)),
				source3.streamTo(streamReducer.newInput(input -> input.key, KeyValue3.REDUCER)),

				streamReducer.getOutput()
						.streamTo(consumer
								.transformWith(decorate(promise -> promise.then(
										item -> Promise.ofException(exception)))))
		);
//		assertEquals(1, list.size());

		assertSame(exception, e);
		assertClosedWithError(source1);
		assertClosedWithError(source2);
		assertClosedWithError(source3);

		assertClosedWithError(streamReducer.getOutput());
	}

	@Test
	public void testSupplierDisconnectWithError() {
		StreamSupplier<KeyValue1> source1 = StreamSuppliers.ofValues(new KeyValue1(1, 10.0), new KeyValue1(3, 30.0));

		Exception exception = new Exception("Test Exception");
		StreamSupplier<KeyValue2> source2 = StreamSuppliers.closingWithError(exception);

		StreamSupplier<KeyValue3> source3 = StreamSuppliers.ofValues(new KeyValue3(2, 10.0, 20.0), new KeyValue3(3, 10.0, 20.0));

		StreamReducer<Integer, KeyValueResult, KeyValueResult> streamReducer = StreamReducer.<Integer, KeyValueResult, KeyValueResult>builder()
				.withBufferSize(1)
				.build();

		ToListStreamConsumer<KeyValueResult> consumer = ToListStreamConsumer.create();

		Exception e = awaitException(
				source1.streamTo(streamReducer.newInput(input -> input.key, KeyValue1.REDUCER)),
				source2.streamTo(streamReducer.newInput(input -> input.key, KeyValue2.REDUCER)),
				source3.streamTo(streamReducer.newInput(input -> input.key, KeyValue3.REDUCER)),

				streamReducer.getOutput().streamTo(consumer)
		);

		assertSame(exception, e);
		assertEquals(0, consumer.getList().size());
		assertClosedWithError(consumer);
		assertClosedWithError(source1);
		assertClosedWithError(source2);
		assertClosedWithError(source3);
	}

	private static final class KeyValue1 {
		public final int key;
		public final double metric1;

		private KeyValue1(int key, double metric1) {
			this.key = key;
			this.metric1 = metric1;
		}

		public static final ReducerToAccumulator<Integer, KeyValue1, KeyValueResult> REDUCER_TO_ACCUMULATOR = new ReducerToAccumulator<>() {
			@Override
			public KeyValueResult createAccumulator(Integer key) {
				return new KeyValueResult(key, 0.0, 0.0, 0.0);
			}

			@Override
			public KeyValueResult accumulate(KeyValueResult accumulator, KeyValue1 value) {
				accumulator.metric1 += value.metric1;
				return accumulator;
			}
		};

		public static final Reducer<Integer, KeyValue1, KeyValueResult, KeyValueResult> REDUCER = new Reducer<>() {
			@Override
			public KeyValueResult onFirstItem(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValue1 firstValue) {
				return new KeyValueResult(key, firstValue.metric1, 0.0, 0.0);
			}

			@Override
			public KeyValueResult onNextItem(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValue1 nextValue, KeyValueResult accumulator) {
				accumulator.metric1 += nextValue.metric1;
				return accumulator;
			}

			@Override
			public void onComplete(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValueResult accumulator) {
				stream.accept(accumulator);
			}

		};
	}

	private static final class KeyValue2 {
		public final int key;
		public final double metric2;

		private KeyValue2(int key, double metric2) {
			this.key = key;
			this.metric2 = metric2;
		}

		public static final ReducerToAccumulator<Integer, KeyValue2, KeyValueResult> REDUCER_TO_ACCUMULATOR = new ReducerToAccumulator<>() {
			@Override
			public KeyValueResult createAccumulator(Integer key) {
				return new KeyValueResult(key, 0.0, 0.0, 0.0);
			}

			@Override
			public KeyValueResult accumulate(KeyValueResult accumulator, KeyValue2 value) {
				accumulator.metric2 += value.metric2;
				return accumulator;
			}
		};

		public static final Reducer<Integer, KeyValue2, KeyValueResult, KeyValueResult> REDUCER = new Reducer<>() {
			@Override
			public KeyValueResult onFirstItem(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValue2 firstValue) {
				return new KeyValueResult(key, 0.0, firstValue.metric2, 0.0);
			}

			@Override
			public KeyValueResult onNextItem(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValue2 nextValue, KeyValueResult accumulator) {
				accumulator.metric2 += nextValue.metric2;
				return accumulator;
			}

			@Override
			public void onComplete(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValueResult accumulator) {
				stream.accept(accumulator);
			}
		};
	}

	private static final class KeyValue3 {
		public final int key;
		public final double metric2;
		public final double metric3;

		private KeyValue3(int key, double metric2, double metric3) {
			this.key = key;
			this.metric2 = metric2;
			this.metric3 = metric3;
		}

		public static final ReducerToAccumulator<Integer, KeyValue3, KeyValueResult> REDUCER_TO_ACCUMULATOR = new ReducerToAccumulator<>() {
			@Override
			public KeyValueResult createAccumulator(Integer key) {
				return new KeyValueResult(key, 0.0, 0.0, 0.0);
			}

			@Override
			public KeyValueResult accumulate(KeyValueResult accumulator, KeyValue3 value) {
				accumulator.metric2 += value.metric2;
				accumulator.metric3 += value.metric3;
				return accumulator;
			}
		};

		public static final Reducer<Integer, KeyValue3, KeyValueResult, KeyValueResult> REDUCER = new Reducer<>() {
			@Override
			public KeyValueResult onFirstItem(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValue3 firstValue) {
				return new KeyValueResult(key, 0.0, firstValue.metric2, firstValue.metric3);
			}

			@Override
			public KeyValueResult onNextItem(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValue3 nextValue, KeyValueResult accumulator) {
				accumulator.metric2 += nextValue.metric2;
				accumulator.metric3 += nextValue.metric3;

				return accumulator;
			}

			@Override
			public void onComplete(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValueResult accumulator) {
				stream.accept(accumulator);
			}
		};
	}

	private static final class KeyValueResult {
		public final int key;
		public double metric1;
		public double metric2;
		public double metric3;

		KeyValueResult(int key, double metric1, double metric2, double metric3) {
			this.key = key;
			this.metric1 = metric1;
			this.metric2 = metric2;
			this.metric3 = metric3;
		}

		@Override
		@SuppressWarnings({"EqualsWhichDoesntCheckParameterClass", "RedundantIfStatement"})
		public boolean equals(Object o) {
			KeyValueResult that = (KeyValueResult) o;

			if (key != that.key) return false;
			if (Double.compare(that.metric1, metric1) != 0) return false;
			if (Double.compare(that.metric2, metric2) != 0) return false;
			if (Double.compare(that.metric3, metric3) != 0) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, metric1, metric2, metric3);
		}

		@Override
		public String toString() {
			return "KeyValueResult{" +
					"key=" + key +
					", metric1=" + metric1 +
					", metric2=" + metric2 +
					", metric3=" + metric3 +
					'}';
		}
	}

	@Test
	public void test2() {
		StreamSupplier<KeyValue1> source1 = StreamSuppliers.ofValues(new KeyValue1(1, 10.0), new KeyValue1(3, 30.0));
		StreamSupplier<KeyValue2> source2 = StreamSuppliers.ofValues(new KeyValue2(1, 10.0), new KeyValue2(3, 30.0));
		StreamSupplier<KeyValue3> source3 = StreamSuppliers.ofValues(new KeyValue3(2, 10.0, 20.0), new KeyValue3(3, 10.0, 20.0));

		StreamReducer<Integer, KeyValueResult, KeyValueResult> streamReducer = StreamReducer.<Integer, KeyValueResult, KeyValueResult>builder()
				.withBufferSize(1)
				.build();

		ToListStreamConsumer<KeyValueResult> consumer = ToListStreamConsumer.create();

		await(
				source1.streamTo(streamReducer.newInput(input -> input.key, KeyValue1.REDUCER_TO_ACCUMULATOR.inputToOutput())),
				source2.streamTo(streamReducer.newInput(input -> input.key, KeyValue2.REDUCER_TO_ACCUMULATOR.inputToOutput())),
				source3.streamTo(streamReducer.newInput(input -> input.key, KeyValue3.REDUCER_TO_ACCUMULATOR.inputToOutput())),

				streamReducer.getOutput().streamTo(consumer.transformWith(randomlySuspending()))
		);

		assertEquals(List.of(
						new KeyValueResult(1, 10.0, 10.0, 0.0),
						new KeyValueResult(2, 0.0, 10.0, 20.0),
						new KeyValueResult(3, 30.0, 40.0, 20.0)),
				consumer.getList());
		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(source3);
	}

	@Test
	public void test3() {
		StreamSupplier<KeyValue1> source1 = StreamSuppliers.ofValues(new KeyValue1(1, 10.0), new KeyValue1(3, 30.0));
		StreamSupplier<KeyValue2> source2 = StreamSuppliers.ofValues(new KeyValue2(1, 10.0), new KeyValue2(3, 30.0));
		StreamSupplier<KeyValue3> source3 = StreamSuppliers.ofValues(new KeyValue3(2, 10.0, 20.0), new KeyValue3(3, 10.0, 20.0));

		StreamReducer<Integer, KeyValueResult, KeyValueResult> streamReducer = StreamReducer.<Integer, KeyValueResult, KeyValueResult>builder()
				.withBufferSize(1)
				.build();

		ToListStreamConsumer<KeyValueResult> consumer = ToListStreamConsumer.create();

		await(
				source1.streamTo(streamReducer.newInput(input -> input.key, KeyValue1.REDUCER)),
				source2.streamTo(streamReducer.newInput(input -> input.key, KeyValue2.REDUCER)),
				source3.streamTo(streamReducer.newInput(input -> input.key, KeyValue3.REDUCER)),

				streamReducer.getOutput()
						.streamTo(consumer.transformWith(randomlySuspending()))
		);

		assertEquals(List.of(
						new KeyValueResult(1, 10.0, 10.0, 0.0),
						new KeyValueResult(2, 0.0, 10.0, 20.0),
						new KeyValueResult(3, 30.0, 40.0, 20.0)),
				consumer.getList());
		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(source3);
	}

	@Test
	public void testDeduplicate() {
		StreamSupplier<Integer> source0 = StreamSuppliers.ofIterable(List.of());
		StreamSupplier<Integer> source1 = StreamSuppliers.ofValues(3, 7);
		StreamSupplier<Integer> source2 = StreamSuppliers.ofValues(3, 4, 6);

		StreamReducer<Integer, Integer, Void> merger = StreamReducer.create();

		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		await(
				source0.streamTo(merger.newInput(identity(), deduplicateReducer())),
				source1.streamTo(merger.newInput(identity(), deduplicateReducer())),
				source2.streamTo(merger.newInput(identity(), deduplicateReducer())),

				merger.getOutput()
						.streamTo(consumer.transformWith(randomlySuspending()))
		);

		assertEquals(List.of(3, 4, 6, 7), consumer.getList());

		assertEndOfStream(source0);
		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(consumer);
		assertEndOfStream(merger.getOutput());
		assertConsumersEndOfStream(merger.getInputs());
	}

	@Test
	public void testDuplicate() {
		StreamSupplier<Integer> source0 = StreamSuppliers.ofIterable(List.of());
		StreamSupplier<Integer> source1 = StreamSuppliers.ofValues(3, 7);
		StreamSupplier<Integer> source2 = StreamSuppliers.ofValues(3, 4, 6);

		StreamReducer<Integer, Integer, Void> merger = StreamReducer.create();

		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		await(
				source0.streamTo(merger.newInput(identity(), mergeReducer())),
				source1.streamTo(merger.newInput(identity(), mergeReducer())),
				source2.streamTo(merger.newInput(identity(), mergeReducer())),
				merger.getOutput()
						.streamTo(consumer.transformWith(randomlySuspending()))
		);

		assertEquals(List.of(3, 3, 4, 6, 7), consumer.getList());

		assertEndOfStream(source0);
		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(consumer);
		assertEndOfStream(merger.getOutput());
		assertConsumersEndOfStream(merger.getInputs());
	}

	@Test
	public void test() {
		DataItem1 d0 = new DataItem1(0, 1, 1, 1);
		DataItem1 d1 = new DataItem1(0, 2, 1, 2);
		DataItem1 d2 = new DataItem1(0, 6, 1, 3);
		DataItem1 d3 = new DataItem1(1, 1, 1, 4);
		DataItem1 d4 = new DataItem1(1, 5, 1, 5);

		StreamSupplier<DataItem1> source1 = StreamSuppliers.ofIterable(
				List.of(d0, //DataItem1(0,1,1,1)
						d1, //DataItem1(0,2,1,2)
						d2  //DataItem1(0,6,1,3)
				));
		StreamSupplier<DataItem1> source2 = StreamSuppliers.ofIterable(
				List.of(d3,//DataItem1(1,1,1,4)
						d4 //DataItem1(1,5,1,5)
				));

		StreamReducer<Integer, DataItem1, Void> merger = StreamReducer.create();

		ToListStreamConsumer<DataItem1> consumer = ToListStreamConsumer.create();

		await(
				source1.streamTo(merger.newInput(DataItem1::key2, mergeReducer())),
				source2.streamTo(merger.newInput(DataItem1::key2, mergeReducer())),
				merger.getOutput()
						.streamTo(consumer.transformWith(oneByOne()))
		);

		assertEquals(List.of(d0, //DataItem1(0,1,1,1)
				d3, //DataItem1(1,1,1,4)
				d1, //DataItem1(0,2,1,2)
				d4, //DataItem1(1,5,1,5)
				d2  //DataItem1(0,6,1,3)
		), consumer.getList());

		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(consumer);
		assertEndOfStream(merger.getOutput());
		assertConsumersEndOfStream(merger.getInputs());
	}

	@Test
	public void testDeduplicateWithError() {
		StreamSupplier<Integer> source1 = StreamSuppliers.ofValues(7, 8);
		StreamSupplier<Integer> source2 = StreamSuppliers.ofValues(3, 4, 6);

		StreamReducer<Integer, Integer, Void> merger = StreamReducer.create();

		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();
		Exception exception = new Exception("Test Exception");

		Exception e = awaitException(
				source1.streamTo(merger.newInput(identity(), deduplicateReducer())),
				source2.streamTo(merger.newInput(identity(), deduplicateReducer())),
				merger.getOutput()
						.streamTo(consumer
								.transformWith(decorate(promise -> promise.then(
										item -> item == 8 ? Promise.ofException(exception) : Promise.of(item)))))
		);

		assertSame(exception, e);
		assertEquals(5, consumer.getList().size());
		assertClosedWithError(source1);
		assertClosedWithError(source2);
		assertClosedWithError(consumer);
		assertClosedWithError(merger.getOutput());
		assertClosedWithError(merger.getInput(0));
		assertClosedWithError(merger.getInput(1));
	}

	@Test
	public void testSupplierDeduplicateWithError() {
		StreamSupplier<Integer> source1 = StreamSuppliers.concat(
				StreamSuppliers.ofValue(7),
				StreamSuppliers.ofValue(8),
				StreamSuppliers.closingWithError(new Exception("Test Exception")),
				StreamSuppliers.ofValue(3),
				StreamSuppliers.ofValue(9)
		);
		StreamSupplier<Integer> source2 = StreamSuppliers.concat(
				StreamSuppliers.ofValue(3),
				StreamSuppliers.ofValue(4),
				StreamSuppliers.ofValue(6),
				StreamSuppliers.ofValue(9)
		);

		StreamReducer<Integer, Integer, Void> merger = StreamReducer.create();

		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		awaitException(
				source1.streamTo(merger.newInput(identity(), deduplicateReducer())),
				source2.streamTo(merger.newInput(identity(), deduplicateReducer())),
				merger.getOutput()
						.streamTo(consumer.transformWith(oneByOne()))
		);

		assertEquals(0, consumer.getList().size());
		assertClosedWithError(consumer);
		assertClosedWithError(merger.getOutput());
		assertClosedWithError(merger.getInput(0));
		assertClosedWithError(merger.getInput(1));
	}

}
