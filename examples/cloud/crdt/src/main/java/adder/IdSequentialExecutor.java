package adder;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.process.AsyncExecutor;
import io.activej.async.process.AsyncExecutors;
import io.activej.promise.Promise;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

public final class IdSequentialExecutor<K> {
	private final Map<K, Tuple> seqExecutors = new HashMap<>();

	public <T> Promise<T> execute(K id, AsyncSupplier<T> supplier) throws RejectedExecutionException {
		Promise<T> result;
		final Tuple finalTuple;

		Tuple tuple = seqExecutors.get(id);
		if (tuple == null) {
			AsyncExecutor executor = AsyncExecutors.sequential();
			result = executor.execute(supplier);
			if (result.isComplete()) return result;

			finalTuple = new Tuple(executor);
			seqExecutors.put(id, finalTuple);
		} else {
			result = tuple.executor.execute(supplier);
			if (result.isComplete()) return result;

			tuple.executionCount++;
			finalTuple = tuple;
		}

		return result
				.whenComplete(() -> {
					if (--finalTuple.executionCount == 0) {
						seqExecutors.remove(id);
					}
				});
	}

	private static final class Tuple {
		private final AsyncExecutor executor;
		private int executionCount = 1;

		private Tuple(AsyncExecutor executor) {
			this.executor = executor;
		}
	}
}
