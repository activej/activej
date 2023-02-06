package io.activej.datastream.supplier.impl;

import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class OfIteratorTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void test1() {
		List<Integer> list = List.of(1, 2, 3);

		StreamSupplier<Integer> supplier = StreamSuppliers.ofIterable(list);
		ToListStreamConsumer<Integer> consumer = ToListStreamConsumer.create();

		await(supplier.streamTo(consumer));

		assertEquals(list, consumer.getList());
		assertEndOfStream(supplier);
		assertEndOfStream(consumer);
	}

}


