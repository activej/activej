package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class ConsumerToListTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void emptyListTest() {
		StreamConsumerToList<String> consumer = StreamConsumerToList.create();

		List<String> testList2 = new ArrayList<>();
		testList2.add("a");
		testList2.add("b");
		testList2.add("c");
		testList2.add("d");

		StreamSupplier<String> supplier = StreamSupplier.ofIterable(testList2);
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
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(testList1);

		List<Integer> testList2 = new ArrayList<>();
		testList2.add(4);
		testList2.add(5);
		testList2.add(6);

		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(testList2);
		await(supplier.streamTo(consumer));

		assertEquals(List.of(1, 2, 3, 4, 5, 6), consumer.getList());
		assertEndOfStream(supplier);
	}

}
