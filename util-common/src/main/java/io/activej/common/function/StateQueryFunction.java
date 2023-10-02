package io.activej.common.function;

import java.util.function.Function;

public interface StateQueryFunction<S> {
	<R> R query(Function<S, R> queryFunction);

	static <S> StateQueryFunction<S> ofState(S state) {
		return new StateQueryFunction<>() {
			@Override
			public <R> R query(Function<S, R> queryFunction) {
				return queryFunction.apply(state);
			}
		};
	}
}
