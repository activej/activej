package io.activej.crdt.storage.cluster;

public interface Sharder<K> {
	int[] shard(K key);

	static <K> Sharder<K> none() {
		return key -> new int[0];
	}

}
