package io.activej.ot;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.process.AsyncCloseable;
import io.activej.promise.Promise;

import java.util.List;
import java.util.function.Function;

public interface StateManager<D, S> {
	Promise<Void> catchUp();

	Promise<Void> push(List<D> diffs);

	StateChangesSupplier<D> subscribeToStateChanges();

	<R> R query(Function<S, R> queryFn);

	interface StateChangesSupplier<D> extends AsyncSupplier<D>, AsyncCloseable {
	}
}
