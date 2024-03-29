package io.activej.etl;

import io.activej.datastream.consumer.StreamConsumerWithResult;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class SplitterLogDataConsumerTest {
	private static final List<Integer> VALUES_1 = IntStream.range(1, 100).boxed().collect(Collectors.toList());
	private static final List<Integer> VALUES_2 = IntStream.range(-100, 0).boxed().collect(Collectors.toList());

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private <T> void assertStreamResult(List<T> values, StreamConsumerWithResult<T, List<T>> consumer, Promise<List<T>> result) {
		await(StreamSuppliers.ofIterable(values).streamTo(consumer.getConsumer()));
		List<T> list = await(result);
		assertEquals(values, list);
	}

	@Test
	public void testConsumes() {
		List<ToListStreamConsumer<Integer>> consumers = List.of(
			ToListStreamConsumer.create(),
			ToListStreamConsumer.create());

		Iterator<ToListStreamConsumer<Integer>> iterator = consumers.iterator();
		Reactor reactor = Reactor.getCurrentReactor();
		SplitterLogDataConsumer<Integer, Integer> splitter =
			new StubSplitter<>(reactor, () -> {
				ToListStreamConsumer<Integer> next = iterator.next();
				return StreamConsumerWithResult.of(next, next.getResult());
			});

		assertStreamResult(VALUES_1, splitter.consume(), consumers.get(0).getResult());
		assertStreamResult(VALUES_2, splitter.consume(), consumers.get(1).getResult());
	}

	@Test
	public void testConsumersWithSuspend() {
		List<ToListStreamConsumer<Integer>> consumers = List.of(
			ToListStreamConsumer.create(),
			ToListStreamConsumer.create());

		Iterator<ToListStreamConsumer<Integer>> iterator = consumers.iterator();
		Reactor reactor = Reactor.getCurrentReactor();
		SplitterLogDataConsumer<Integer, Integer> splitter =
			new StubSplitter<>(reactor, () -> {
				ToListStreamConsumer<Integer> next = iterator.next();
				return StreamConsumerWithResult.of(next, next.getResult());
			});

		assertStreamResult(VALUES_1, splitter.consume(), consumers.get(0).getResult());
		assertStreamResult(VALUES_2, splitter.consume(), consumers.get(1).getResult());
	}

	@Test(expected = IllegalStateException.class)
	public void testIncorrectImplementation() {
		Reactor reactor = Reactor.getCurrentReactor();
		SplitterLogDataConsumer<Integer, Integer> splitter = new SplitterLogDataConsumer<>(reactor) {
			@Override
			protected StreamDataAcceptor<Integer> createSplitter(Context ctx) {
				return item -> {};
			}
		};

		StreamSuppliers.ofIterable(VALUES_1).streamTo(splitter.consume().getConsumer());
	}

	private static class StubSplitter<T, D> extends SplitterLogDataConsumer<T, D> {
		private final ILogDataConsumer<T, D> logConsumer;

		private StubSplitter(Reactor reactor, ILogDataConsumer<T, D> logConsumer) {
			super(reactor);
			this.logConsumer = logConsumer;
		}

		@Override
		protected StreamDataAcceptor<T> createSplitter(Context ctx) {
			return ctx.addOutput(logConsumer);
		}
	}

}
