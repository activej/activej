package io.activej.etl;

import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamConsumerWithResult;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class LogDataConsumerSplitterTest {
	private static final List<Integer> VALUES_1 = IntStream.range(1, 100).boxed().collect(Collectors.toList());
	private static final List<Integer> VALUES_2 = IntStream.range(-100, 0).boxed().collect(Collectors.toList());

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private <T> void assertStreamResult(List<T> values, StreamConsumerWithResult<T, List<T>> consumer, Promise<List<T>> result) {
		await(StreamSupplier.ofIterable(values).streamTo(consumer.getConsumer()));
		List<T> list = await(result);
		assertEquals(values, list);
	}

	@Test
	public void testConsumes() {
		List<StreamConsumerToList<Integer>> consumers = List.of(
				StreamConsumerToList.create(),
				StreamConsumerToList.create());

		Iterator<StreamConsumerToList<Integer>> iterator = consumers.iterator();
		LogDataConsumerSplitter<Integer, Integer> splitter =
				new LogDataConsumerSplitterStub<>(() -> {
					StreamConsumerToList<Integer> next = iterator.next();
					return StreamConsumerWithResult.of(next, next.getResult());
				});

		assertStreamResult(VALUES_1, splitter.consume(), consumers.get(0).getResult());
		assertStreamResult(VALUES_2, splitter.consume(), consumers.get(1).getResult());
	}

	@Test
	public void testConsumersWithSuspend() {
		List<StreamConsumerToList<Integer>> consumers = List.of(
				StreamConsumerToList.create(),
				StreamConsumerToList.create());

		Iterator<StreamConsumerToList<Integer>> iterator = consumers.iterator();
		LogDataConsumerSplitter<Integer, Integer> splitter =
				new LogDataConsumerSplitterStub<>(() -> {
					StreamConsumerToList<Integer> next = iterator.next();
					return StreamConsumerWithResult.of(next, next.getResult());
				});

		assertStreamResult(VALUES_1, splitter.consume(), consumers.get(0).getResult());
		assertStreamResult(VALUES_2, splitter.consume(), consumers.get(1).getResult());
	}

	@Test(expected = IllegalStateException.class)
	public void testIncorrectImplementation() {
		LogDataConsumerSplitter<Integer, Integer> splitter = new LogDataConsumerSplitter<>() {
			@Override
			protected StreamDataAcceptor<Integer> createSplitter(@NotNull Context ctx) {
				return item -> {};
			}
		};

		StreamSupplier.ofIterable(VALUES_1).streamTo(splitter.consume().getConsumer());
	}

	private static class LogDataConsumerSplitterStub<T, D> extends LogDataConsumerSplitter<T, D> {
		private final LogDataConsumer<T, D> logConsumer;

		private LogDataConsumerSplitterStub(LogDataConsumer<T, D> logConsumer) {
			this.logConsumer = logConsumer;
		}

		@Override
		protected StreamDataAcceptor<T> createSplitter(@NotNull Context ctx) {
			return ctx.addOutput(logConsumer);
		}
	}

}
