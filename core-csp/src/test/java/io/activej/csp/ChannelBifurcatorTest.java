package io.activej.csp;

import io.activej.async.function.AsyncConsumer;
import io.activej.common.exception.StacklessException;
import io.activej.csp.process.ChannelBifurcator;
import io.activej.eventloop.Eventloop;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.assertComplete;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ChannelBifurcatorTest {

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	//[START REGION_1]
	public void simpleCase() {
		List<String> expected = new ArrayList<>();
		expected.add("1st");
		expected.add("2nd");
		expected.add("3rd");
		expected.add("4th");
		expected.add("5th");

		Eventloop eventloop = Eventloop.create().withCurrentThread();

		List<String> firstResults = new ArrayList<>();
		List<String> secondResults = new ArrayList<>();

		ChannelBifurcator.<String>create()
				.withInput(ChannelSupplier.ofIterable(expected))
				.withOutputs(ChannelConsumer.of(AsyncConsumer.<String>of(firstResults::add)).async(),
						ChannelConsumer.of(AsyncConsumer.<String>of(secondResults::add)).async())
				.getProcessCompletion()
				.whenComplete(assertComplete());

		eventloop.run();

		assertEquals(expected, firstResults);
		assertEquals(firstResults, secondResults);
		assertEquals(expected, secondResults);
	}
	//[END REGION_1]

	@Test
	public void outputFailure() {

		List<String> expected = new ArrayList<>();
		expected.add("first");
		expected.add("second");
		expected.add("third");

		List<String> results = new ArrayList<>();

		StacklessException exception = new StacklessException(ChannelBifurcator.class, "test exception");
		ChannelBifurcator<String> bifurcator = ChannelBifurcator.<String>create()
				.withInput(ChannelSupplier.ofIterable(expected))
				.withOutputs(ChannelConsumer.of(AsyncConsumer.<String>of(results::add)).async(),
						ChannelConsumer.ofException(exception));

		assertSame(exception, awaitException(bifurcator.startProcess()));
	}
}
