package io.activej.etcd.codec;

import io.activej.common.exception.MalformedDataException;
import io.etcd.jetcd.ByteSequence;

public interface EtcdValueCodec<V> extends EtcdValueEncoder<V>, EtcdValueDecoder<V> {

	static <V> EtcdValueCodec<V> of(EtcdValueEncoder<V> encoder, EtcdValueDecoder<V> decoder) {
		return new EtcdValueCodec<V>() {
			@Override
			public ByteSequence encodeValue(V value) {
				return encoder.encodeValue(value);
			}

			@Override
			public V decodeValue(ByteSequence byteSequence) throws MalformedDataException {
				return decoder.decodeValue(byteSequence);
			}
		};
	}

}
