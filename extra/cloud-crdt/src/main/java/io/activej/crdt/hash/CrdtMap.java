package io.activej.crdt.hash;

import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

public interface CrdtMap<K extends Comparable<K>, S> {
	Promise<@Nullable S> put(K key, S value);

	Promise<@Nullable S> get(K key);
}
