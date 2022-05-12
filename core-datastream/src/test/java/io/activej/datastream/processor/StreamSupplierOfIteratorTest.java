package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class StreamSupplierOfIteratorTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void test1() {
		List<Integer> list = List.of(1, 2, 3);

		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(list);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(supplier.streamTo(consumer));

		assertEquals(list, consumer.getList());
		assertEndOfStream(supplier);
		assertEndOfStream(consumer);
	}

}


