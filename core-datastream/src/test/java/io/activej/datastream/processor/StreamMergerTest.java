package io.activej.datastream.processor;

import io.activej.datastream.StreamSupplier;
import io.activej.datastream.ToListStreamConsumer;
import io.activej.promise.Promise;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.activej.datastream.TestStreamTransformers.*;
import static io.activej.datastream.TestUtils.*;
import static io.activej.datastream.processor.reducer.Reducers.deduplicateReducer;
import static io.activej.datastream.processor.reducer.Reducers.mergeReducer;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.function.Function.identity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamMergerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testDeduplicate() {
		StreamSupplier<Integer> source0 = StreamSupplier.ofIterable(List.of());
		StreamSupplier<Integer> source1 = StreamSupplier.of(3, 7);
		StreamSupplier<Integer> source2 = StreamSupplier.of(3, 4, 6);

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
		StreamSupplier<Integer> source0 = StreamSupplier.ofIterable(List.of());
		StreamSupplier<Integer> source1 = StreamSupplier.of(3, 7);
		StreamSupplier<Integer> source2 = StreamSupplier.of(3, 4, 6);

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

		StreamSupplier<DataItem1> source1 = StreamSupplier.ofIterable(
				List.of(d0, //DataItem1(0,1,1,1)
						d1, //DataItem1(0,2,1,2)
						d2  //DataItem1(0,6,1,3)
				));
		StreamSupplier<DataItem1> source2 = StreamSupplier.ofIterable(
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
		StreamSupplier<Integer> source1 = StreamSupplier.of(7, 8);
		StreamSupplier<Integer> source2 = StreamSupplier.of(3, 4, 6);

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
		StreamSupplier<Integer> source1 = StreamSupplier.concat(
				StreamSupplier.of(7),
				StreamSupplier.of(8),
				StreamSupplier.closingWithError(new Exception("Test Exception")),
				StreamSupplier.of(3),
				StreamSupplier.of(9)
		);
		StreamSupplier<Integer> source2 = StreamSupplier.concat(
				StreamSupplier.of(3),
				StreamSupplier.of(4),
				StreamSupplier.of(6),
				StreamSupplier.of(9)
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
