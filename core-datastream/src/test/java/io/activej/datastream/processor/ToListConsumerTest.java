package io.activej.datastream.processor;

import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class ToListConsumerTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void emptyListTest() {
		ToListStreamConsumer<String> consumer = ToListStreamConsumer.create();

		List<String> testList2 = new ArrayList<>();
		testList2.add("a");
		testList2.add("b");
		testList2.add("c");
		testList2.add("d");

		StreamSupplier<String> supplier = StreamSuppliers.ofIterable(testList2);
		await(supplier.streamTo(consumer));

		assertEquals(testList2, consumer.getList());
		assertEndOfStream(supplier);
	}

	@Test
	public void fullListTest() {
		List<Integer> testList1 = new ArrayList<>();
		testList1.add(1);
		testList1.add(2);
		testList1.add(3);
		StreamConsumer<Integer> consumer = ToListStreamConsumer.create(testList1);

		List<Integer> testList2 = new ArrayList<>();
		testList2.add(4);
		testList2.add(5);
		testList2.add(6);

		StreamSupplier<Integer> supplier = StreamSuppliers.ofIterable(testList2);
		await(supplier.streamTo(consumer));

		assertEquals(List.of(1, 2, 3, 4, 5, 6), testList1);
		assertEndOfStream(supplier);
	}

}
