package io.activej.datastream.processor.join;

import io.activej.datastream.supplier.StreamDataAcceptor;
import org.jetbrains.annotations.Nullable;

/**
 * Simple implementation of Joiner, which does left inner and left outer join
 */
public abstract class ValueLeftJoiner<K, L, R, V> implements LeftJoiner<K, L, R, V> {
	/**
	 * Method which contains implementation for left inner join.
	 */
	public abstract @Nullable V doInnerJoin(K key, L left, R right);

	/**
	 * Method which contains implementation for left outer join
	 */
	public @Nullable V doOuterJoin(K key, L left) {
		return null;
	}

	@Override
	public final void onInnerJoin(K key, L left, R right, StreamDataAcceptor<V> output) {
		V result = doInnerJoin(key, left, right);
		if (result != null) {
			output.accept(result);
		}
	}

	@Override
	public final void onOuterJoin(K key, L left, StreamDataAcceptor<V> output) {
		V result = doOuterJoin(key, left);
		if (result != null) {
			output.accept(result);
		}
	}
}
