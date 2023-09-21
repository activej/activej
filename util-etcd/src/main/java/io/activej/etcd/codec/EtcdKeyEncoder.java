package io.activej.etcd.codec;

import io.etcd.jetcd.ByteSequence;

public interface EtcdKeyEncoder<K> {
	ByteSequence encodeKey(K key);
}
