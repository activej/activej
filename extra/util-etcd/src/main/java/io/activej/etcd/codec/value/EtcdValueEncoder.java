package io.activej.etcd.codec.value;

import io.etcd.jetcd.ByteSequence;

public interface EtcdValueEncoder<V> {
	ByteSequence encodeValue(V value);
}
