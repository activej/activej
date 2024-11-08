package io.activej.etcd.codec.key;

import io.activej.etcd.exception.MalformedEtcdDataException;
import io.etcd.jetcd.ByteSequence;

public interface EtcdKeyCodec<K> extends EtcdKeyEncoder<K>, EtcdKeyDecoder<K> {

	static <K> EtcdKeyCodec<K> of(EtcdKeyEncoder<K> encoder, EtcdKeyDecoder<K> decoder) {
		return new EtcdKeyCodec<>() {
			@Override
			public ByteSequence encodeKey(K key) {
				return encoder.encodeKey(key);
			}

			@Override
			public K decodeKey(ByteSequence byteSequence) throws MalformedEtcdDataException {
				return decoder.decodeKey(byteSequence);
			}
		};
	}

}
