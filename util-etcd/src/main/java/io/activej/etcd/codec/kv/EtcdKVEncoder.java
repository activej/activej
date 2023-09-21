package io.activej.etcd.codec.kv;

import io.activej.etcd.codec.key.EtcdKeyEncoder;

public interface EtcdKVEncoder<K, KV> extends EtcdKeyEncoder<K> {
	KeyValue encodeKV(KV kv);
}
