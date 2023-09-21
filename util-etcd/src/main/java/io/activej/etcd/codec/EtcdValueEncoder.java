package io.activej.etcd.codec;

import io.etcd.jetcd.ByteSequence;

public interface EtcdValueEncoder<V> {
	ByteSequence encodeValue(V value);
}
