package io.activej.etcd.codec;

import io.etcd.jetcd.ByteSequence;

public interface EtcdPrefixEncoder<K> {
	ByteSequence encodePrefix(K key, ByteSequence suffix);
}
