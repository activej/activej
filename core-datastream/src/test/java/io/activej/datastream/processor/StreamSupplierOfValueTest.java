package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumer_ToList;
import io.activej.datastream.StreamSupplier;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StreamSupplierOfValueTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private static final String TEST_STRING = "Hello consumer";
	private static final Integer TEST_INT = 777;
	private static final DataItem1 TEST_OBJECT = new DataItem1(1, 1, 8, 8);
	private static final Object TEST_NULL = null;

	@Test
	public void test1() {
		StreamConsumer_ToList<Integer> consumer1 = StreamConsumer_ToList.create();
		StreamSupplier<Integer> supplier1 = StreamSupplier.of(TEST_INT);
		await(supplier1.streamTo(consumer1));

		assertEquals(TEST_INT, consumer1.getList().get(0));
		assertEndOfStream(supplier1);

		StreamConsumer_ToList<String> consumer2 = StreamConsumer_ToList.create();
		StreamSupplier<String> supplier2 = StreamSupplier.of(TEST_STRING);
		await(supplier2.streamTo(consumer2));

		assertEquals(TEST_STRING, consumer2.getList().get(0));
		assertEndOfStream(supplier2);

		StreamConsumer_ToList<DataItem1> consumer3 = StreamConsumer_ToList.create();
		StreamSupplier<DataItem1> supplier3 = StreamSupplier.of(TEST_OBJECT);
		await(supplier3.streamTo(consumer3));

		assertEquals(TEST_OBJECT, consumer3.getList().get(0));
		assertEndOfStream(supplier3);
	}

	@Test
	public void testNull() {
		StreamConsumer_ToList<Object> consumer3 = StreamConsumer_ToList.create();
		StreamSupplier<Object> supplier3 = StreamSupplier.of(TEST_NULL);
		await(supplier3.streamTo(consumer3));

		assertNull(consumer3.getList().get(0));
		assertEndOfStream(supplier3);
	}

}
