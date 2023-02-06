package io.activej.datastream.processor.transformer;

import io.activej.common.annotation.StaticFactories;
import io.activej.datastream.processor.transformer.impl.*;

import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.common.Checks.checkState;

@StaticFactories(StreamTransformer.class)
public class StreamTransformers {

	/**
	 * An identity transformer that does not change the elements.
	 */
	public static <T> StreamTransformer<T, T> identity() {
		return mapper(Function.identity());
	}

	public static <T> StreamTransformer<T, T> skip(long skip) {
		checkState(skip >= Skip.NO_SKIP, "Skip value cannot be a negative value");

		return skip == Skip.NO_SKIP ?
				identity() :
				new Skip<>(skip);
	}

	public static <T> StreamTransformer<T, T> limit(long limit) {
		checkState(limit >= Limiter.NO_LIMIT, "Limit cannot be a negative value");

		return limit == Limiter.NO_LIMIT ?
				identity() :
				new Limiter<>(limit);
	}

	public static <T> StreamTransformer<T, T> filter(Predicate<T> predicate) {
		return new Filter<>(predicate);
	}

	public static <I, O> StreamTransformer<I, O> mapper(Function<I, O> mapFn) {
		return new Mapper<>(mapFn);
	}

	public static <T> StreamTransformer<T, T> buffer(int bufferMinSize, int bufferMaxSize) {
		return new Buffer<>(bufferMinSize, bufferMaxSize);
	}

}
