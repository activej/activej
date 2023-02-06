package io.activej.datastream.processor.transformer.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.datastream.processor.transformer.AbstractStreamTransformer;
import io.activej.datastream.supplier.StreamDataAcceptor;

import java.util.function.Function;

@ExposedInternals
public final class Mapper<I, O> extends AbstractStreamTransformer<I, O> {
	public final Function<I, O> mapFn;

	public Mapper(Function<I, O> mapFn) {this.mapFn = mapFn;}

	@Override
	protected StreamDataAcceptor<I> onResumed(StreamDataAcceptor<O> output) {
		return item -> output.accept(mapFn.apply(item));
	}
}
