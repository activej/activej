package io.activej.etcd.codec.prefix;

import io.etcd.jetcd.ByteSequence;

public interface EtcdPrefixEncoder<K> {
	ByteSequence encodePrefix(Prefix<K> prefix);
}
