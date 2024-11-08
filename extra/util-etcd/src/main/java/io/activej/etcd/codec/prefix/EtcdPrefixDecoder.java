package io.activej.etcd.codec.prefix;

import io.activej.etcd.exception.MalformedEtcdDataException;
import io.etcd.jetcd.ByteSequence;

public interface EtcdPrefixDecoder<K> {
	Prefix<K> decodePrefix(ByteSequence byteSequence) throws MalformedEtcdDataException;
}
