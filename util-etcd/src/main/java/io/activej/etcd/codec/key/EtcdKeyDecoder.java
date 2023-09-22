package io.activej.etcd.codec.key;

import io.activej.etcd.exception.MalformedEtcdDataException;
import io.etcd.jetcd.ByteSequence;

public interface EtcdKeyDecoder<K> {
	K decodeKey(ByteSequence byteSequence) throws MalformedEtcdDataException;
}
