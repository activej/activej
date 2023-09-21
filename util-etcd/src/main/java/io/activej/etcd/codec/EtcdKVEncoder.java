package io.activej.etcd.codec;

public interface EtcdKVEncoder<K, KV> extends EtcdKeyEncoder<K> {
	KeyValue encodeKV(KV kv);
}
