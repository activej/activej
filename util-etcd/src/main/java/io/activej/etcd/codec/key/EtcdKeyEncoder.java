package io.activej.etcd.codec.key;

import io.etcd.jetcd.ByteSequence;

public interface EtcdKeyEncoder<K> {
	ByteSequence encodeKey(K key);
}
