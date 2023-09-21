package io.activej.etcd.codec.prefix;

import io.activej.common.exception.MalformedDataException;
import io.etcd.jetcd.ByteSequence;

public interface EtcdPrefixDecoder<K> {
	Prefix<K> decodePrefix(ByteSequence byteSequence) throws MalformedDataException;
}
