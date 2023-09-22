package io.activej.etcd.codec.value;

import io.activej.etcd.exception.MalformedEtcdDataException;
import io.etcd.jetcd.ByteSequence;

public interface EtcdValueDecoder<V> {
	V decodeValue(ByteSequence byteSequence) throws MalformedEtcdDataException;
}
