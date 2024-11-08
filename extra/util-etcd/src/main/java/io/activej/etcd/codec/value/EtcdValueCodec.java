package io.activej.etcd.codec.value;

import io.activej.etcd.exception.MalformedEtcdDataException;
import io.etcd.jetcd.ByteSequence;

public interface EtcdValueCodec<V> extends EtcdValueEncoder<V>, EtcdValueDecoder<V> {

	static <V> EtcdValueCodec<V> of(EtcdValueEncoder<V> encoder, EtcdValueDecoder<V> decoder) {
		return new EtcdValueCodec<>() {
			@Override
			public ByteSequence encodeValue(V value) {
				return encoder.encodeValue(value);
			}

			@Override
			public V decodeValue(ByteSequence byteSequence) throws MalformedEtcdDataException {
				return decoder.decodeValue(byteSequence);
			}
		};
	}

}
