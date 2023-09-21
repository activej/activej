package io.activej.etcd.codec.value;

import io.activej.common.exception.MalformedDataException;
import io.etcd.jetcd.ByteSequence;

public interface EtcdValueDecoder<V> {
	V decodeValue(ByteSequence byteSequence) throws MalformedDataException;
}
