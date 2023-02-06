package io.activej.datastream.supplier.impl;

import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.processor.DataItem1;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OfValueTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private static final String TEST_STRING = "Hello consumer";
	private static final Integer TEST_INT = 777;
	private static final DataItem1 TEST_OBJECT = new DataItem1(1, 1, 8, 8);
	private static final Object TEST_NULL = null;

	@Test
	public void test1() {
		ToListStreamConsumer<Integer> consumer1 = ToListStreamConsumer.create();
		StreamSupplier<Integer> supplier1 = StreamSuppliers.ofValue(TEST_INT);
		await(supplier1.streamTo(consumer1));

		assertEquals(TEST_INT, consumer1.getList().get(0));
		assertEndOfStream(supplier1);

		ToListStreamConsumer<String> consumer2 = ToListStreamConsumer.create();
		StreamSupplier<String> supplier2 = StreamSuppliers.ofValue(TEST_STRING);
		await(supplier2.streamTo(consumer2));

		assertEquals(TEST_STRING, consumer2.getList().get(0));
		assertEndOfStream(supplier2);

		ToListStreamConsumer<DataItem1> consumer3 = ToListStreamConsumer.create();
		StreamSupplier<DataItem1> supplier3 = StreamSuppliers.ofValue(TEST_OBJECT);
		await(supplier3.streamTo(consumer3));

		assertEquals(TEST_OBJECT, consumer3.getList().get(0));
		assertEndOfStream(supplier3);
	}

	@Test
	public void testNull() {
		ToListStreamConsumer<Object> consumer3 = ToListStreamConsumer.create();
		StreamSupplier<Object> supplier3 = StreamSuppliers.ofValue(TEST_NULL);
		await(supplier3.streamTo(consumer3));

		assertNull(consumer3.getList().get(0));
		assertEndOfStream(supplier3);
	}

}
