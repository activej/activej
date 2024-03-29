package io.activej.datastream.processor;

import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static io.activej.datastream.TestStreamTransformers.*;
import static io.activej.datastream.TestUtils.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamUnionTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void test1() {
		StreamUnion<Integer> streamUnion = StreamUnion.create();

		StreamSupplier<Integer> source0 = StreamSuppliers.empty();
		StreamSupplier<Integer> source1 = StreamSuppliers.ofValue(1);
		StreamSupplier<Integer> source2 = StreamSuppliers.ofValues(2, 3);
		StreamSupplier<Integer> source3 = StreamSuppliers.empty();
		StreamSupplier<Integer> source4 = StreamSuppliers.ofValues(4, 5);
		StreamSupplier<Integer> source5 = StreamSuppliers.ofValue(6);
		StreamSupplier<Integer> source6 = StreamSuppliers.empty();

		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		await(
			source0.streamTo(streamUnion.newInput()),
			source1.streamTo(streamUnion.newInput()),
			source2.streamTo(streamUnion.newInput()),
			source3.streamTo(streamUnion.newInput()),
			source4.streamTo(streamUnion.newInput()),
			source5.streamTo(streamUnion.newInput()),
			source6.streamTo(streamUnion.newInput()),

			streamUnion.getOutput()
				.streamTo(consumer.transformWith(randomlySuspending()))
		);

		List<Integer> result = consumer.getList();
		Collections.sort(result);
		assertEquals(List.of(1, 2, 3, 4, 5, 6), result);

		assertEndOfStream(source0);
		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(source3);
		assertEndOfStream(source4);
		assertEndOfStream(source5);
		assertEndOfStream(source6);

		assertEndOfStream(streamUnion.getOutput());
		assertConsumersEndOfStream(streamUnion.getInputs());
	}

	@Test
	public void testWithError() {
		StreamUnion<Integer> streamUnion = StreamUnion.create();

		StreamSupplier<Integer> source0 = StreamSuppliers.ofValues(1, 2, 3);
		StreamSupplier<Integer> source1 = StreamSuppliers.ofValues(4, 5);
		StreamSupplier<Integer> source2 = StreamSuppliers.ofValues(6, 7);

		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();
		ExpectedException exception = new ExpectedException("Test Exception");

		Exception e = awaitException(
			source0.streamTo(streamUnion.newInput()),
			source1.streamTo(streamUnion.newInput()),
			source2.streamTo(streamUnion.newInput()),

			streamUnion.getOutput()
				.streamTo(consumer
					.transformWith(decorate(promise ->
						promise.then(item -> item == 3 ? Promise.ofException(exception) : Promise.of(item)))))
		);

		assertSame(exception, e);
		assertEquals(List.of(1, 2, 3), consumer.getList());
		assertClosedWithError(source0);
		assertClosedWithError(source1);
		assertClosedWithError(source2);

		assertClosedWithError(streamUnion.getOutput());
		assertClosedWithError(streamUnion.getInput(0));
		assertClosedWithError(streamUnion.getInput(1));
		assertClosedWithError(streamUnion.getInput(2));
	}

	@Test
	public void testSupplierWithError() {
		StreamUnion<Integer> streamUnion = StreamUnion.create();
		ExpectedException exception = new ExpectedException("Test Exception");

		StreamSupplier<Integer> source0 = StreamSuppliers.concat(
			StreamSuppliers.ofIterable(List.of(1, 2)),
			StreamSuppliers.closingWithError(exception)
		);
		StreamSupplier<Integer> source1 = StreamSuppliers.concat(
			StreamSuppliers.ofIterable(List.of(7, 8, 9)),
			StreamSuppliers.closingWithError(exception)
		);

		StreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		Exception e = awaitException(
			source0.streamTo(streamUnion.newInput()),
			source1.streamTo(streamUnion.newInput()),
			streamUnion.getOutput()
				.streamTo(consumer.transformWith(oneByOne()))
		);

		assertSame(exception, e);
//		assertEquals(3, list.size());
		assertClosedWithError(streamUnion.getOutput());
		assertConsumersClosedWithError(streamUnion.getInputs());
	}

}
