package io.activej.etcd;

public interface EtcdEventProcessor<K, KV, A> {
	A createEventsAccumulator();

	void onPut(A accumulator, KV kv);

	void onDelete(A accumulator, K key);
}
