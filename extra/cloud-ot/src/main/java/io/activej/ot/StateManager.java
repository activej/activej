package io.activej.ot;

import io.activej.promise.Promise;

import java.util.List;
import java.util.function.Function;

public interface StateManager<D, S> {
	Promise<Void> catchUp();

	Promise<Void> push(List<D> diffs);

	<R> R query(Function<S, R> queryFn);
}
