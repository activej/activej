package io.activej.crdt.wal;

import io.activej.promise.Promise;

public interface WriteAheadLog<K extends Comparable<K>, S> {
	Promise<Void> put(K key, S value);

	Promise<Void> flush();
}
