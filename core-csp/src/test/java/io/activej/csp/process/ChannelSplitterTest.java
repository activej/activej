package io.activej.csp.process;

import io.activej.async.function.AsyncConsumer;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ChannelSplitterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void simpleCase() {
		int n = 10;

		List<String> expected = new ArrayList<>();
		expected.add("first");
		expected.add("second");
		expected.add("third");

		List<String> theList = new ArrayList<>();

		ChannelSplitter<String> splitter = ChannelSplitter.<String>create()
			.withInput(ChannelSuppliers.ofList(expected));

		for (int i = 0; i < n; i++) {
			splitter.addOutput()
				.set(ChannelConsumers.ofAsyncConsumer(AsyncConsumer.<String>of(theList::add)).async());
		}

		await(splitter.startProcess());

		assertEquals(expected.stream().flatMap(x -> Stream.generate(() -> x).limit(n)).collect(toList()), theList);
	}

	@Test
	public void inputFailure() {
		int n = 10;

		List<String> expected = new ArrayList<>();
		expected.add("first");
		expected.add("second");
		expected.add("third");

		Exception exception = new Exception("test exception");
		ChannelSplitter<String> splitter = ChannelSplitter.<String>create()
			.withInput(ChannelSuppliers.concat(ChannelSuppliers.ofList(expected), ChannelSuppliers.ofException(exception)));

		for (int i = 0; i < n; i++) {
			splitter.addOutput()
				.set(ChannelConsumers.ofAsyncConsumer(AsyncConsumer.of((String s) -> { /*noop*/ })).async());
		}

		assertSame(exception, awaitException(splitter.startProcess()));
	}

	@Test
	public void oneOutputFailure() {
		int n = 10;

		List<String> expected = new ArrayList<>();
		expected.add("first");
		expected.add("second");
		expected.add("third");

		ChannelSplitter<String> splitter = ChannelSplitter.<String>create()
			.withInput(ChannelSuppliers.ofList(expected));
		Exception exception = new Exception("test exception");

		for (int i = 0; i < n; i++) {
			if (i == n / 2) {
				splitter.addOutput()
					.set(ChannelConsumers.ofException(exception));
			} else {
				splitter.addOutput()
					.set(ChannelConsumers.ofAsyncConsumer(AsyncConsumer.of((String s) -> { /*noop*/ })).async());
			}
		}

		assertSame(exception, awaitException(splitter.startProcess()));
	}
}
