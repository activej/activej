package io.activej.etcd.codec.kv;

import io.activej.common.exception.MalformedDataException;
import io.activej.etcd.codec.key.EtcdKeyCodec;
import io.etcd.jetcd.ByteSequence;

public interface EtcdKVCodec<K, KV> extends EtcdKVEncoder<K, KV>, EtcdKVDecoder<K, KV>, EtcdKeyCodec<K> {

	static <K, KV> EtcdKVCodec<K, KV> of(EtcdKVEncoder<K, KV> encoder, EtcdKVDecoder<K, KV> decoder) {
		return new EtcdKVCodec<>() {
			@Override
			public KeyValue encodeKV(KV kv) {
				return encoder.encodeKV(kv);
			}

			@Override
			public ByteSequence encodeKey(K key) {
				return encoder.encodeKey(key);
			}

			@Override
			public KV decodeKV(KeyValue kv) throws MalformedDataException {
				return decoder.decodeKV(kv);
			}

			@Override
			public K decodeKey(ByteSequence byteSequence) throws MalformedDataException {
				return decoder.decodeKey(byteSequence);
			}
		};
	}
}
