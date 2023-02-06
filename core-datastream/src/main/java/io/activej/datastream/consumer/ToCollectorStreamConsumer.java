package io.activej.datastream.consumer;

import io.activej.promise.SettablePromise;

import java.util.function.BiConsumer;
import java.util.stream.Collector;

public final class ToCollectorStreamConsumer<T, A, R> extends AbstractStreamConsumer<T> {
	private final SettablePromise<R> resultPromise = new SettablePromise<>();
	private final Collector<T, A, R> collector;

	private A accumulator;

	private ToCollectorStreamConsumer(Collector<T, A, R> collector) {
		this.collector = collector;
	}

	public static <T, A, R> ToCollectorStreamConsumer<T, A, R> create(Collector<T, A, R> collector) {
		return new ToCollectorStreamConsumer<>(collector);
	}

	public SettablePromise<R> getResultPromise() {
		return resultPromise;
	}

	@Override
	protected void onInit() {
		resultPromise.whenResult(this::acknowledge);
	}

	@Override
	protected void onStarted() {
		A accumulator = collector.supplier().get();
		this.accumulator = accumulator;
		BiConsumer<A, T> consumer = collector.accumulator();
		resume(item -> consumer.accept(accumulator, item));
	}

	@Override
	protected void onEndOfStream() {
		resultPromise.set(collector.finisher().apply(accumulator));
	}

	@Override
	protected void onError(Exception e) {
		resultPromise.setException(e);
	}

	@Override
	protected void onCleanup() {
		accumulator = null;
	}

}
