package io.activej.datastream.processor.transformer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.processor.transformer.AbstractStreamTransformer;
import io.activej.datastream.supplier.StreamDataAcceptor;

import java.util.function.Predicate;

@ExposedInternals
public final class Filter<T> extends AbstractStreamTransformer<T, T> {
	public final Predicate<T> predicate;

	public Filter(Predicate<T> predicate) {this.predicate = predicate;}

	@Override
	protected StreamDataAcceptor<T> onResumed(StreamDataAcceptor<T> output) {
		return item -> {if (predicate.test(item)) output.accept(item);};
	}
}
