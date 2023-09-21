package io.activej.etcd.codec.key;

import io.activej.common.exception.MalformedDataException;
import io.etcd.jetcd.ByteSequence;

public interface EtcdKeyDecoder<K> {
	K decodeKey(ByteSequence byteSequence) throws MalformedDataException;
}
