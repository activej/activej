package io.activej.etcd.codec.kv;

import io.activej.common.exception.MalformedDataException;
import io.activej.common.function.DecoderFunction;
import io.activej.common.tuple.Tuple2;
import io.activej.etcd.codec.key.EtcdKeyCodec;
import io.activej.etcd.codec.prefix.EtcdPrefixCodec;
import io.activej.etcd.codec.prefix.Prefix;
import io.activej.etcd.codec.value.EtcdValueCodec;
import io.etcd.jetcd.ByteSequence;

import java.util.Map;
import java.util.function.Function;

public class EtcdKVCodecs {

	public static <K, V> EtcdKVCodec<K, Map.Entry<K, V>> ofMapEntry(EtcdKeyCodec<K> keyCodec, EtcdValueCodec<V> valueCodec) {
		return new EtcdKVCodec<>() {
			@Override
			public KeyValue encodeKV(Map.Entry<K, V> kv) {
				return new KeyValue(keyCodec.encodeKey(kv.getKey()), valueCodec.encodeValue(kv.getValue()));
			}

			@Override
			public ByteSequence encodeKey(K key) {
				return keyCodec.encodeKey(key);
			}

			@Override
			public Map.Entry<K, V> decodeKV(KeyValue kv) throws MalformedDataException {
				return Map.entry(keyCodec.decodeKey(kv.key()), valueCodec.decodeValue(kv.value()));
			}

			@Override
			public K decodeKey(ByteSequence byteSequence) throws MalformedDataException {
				return keyCodec.decodeKey(byteSequence);
			}
		};
	}

	public static <K0, K, T> EtcdKVCodec<Tuple2<K0, K>, Tuple2<K0, T>> ofPrefixedEntry(EtcdPrefixCodec<K0> prefixCodec, Function<K0, EtcdKVCodec<K, T>> codecs) {
		return new EtcdKVCodec<>() {
			@Override
			public Tuple2<K0, T> decodeKV(KeyValue kv) throws MalformedDataException {
				Prefix<K0> prefix = prefixCodec.decodePrefix(kv.key());
				EtcdKVCodec<K, T> codec = codecs.apply(prefix.key());
				if (codec == null) {
					throw new MalformedDataException("Unexpected key: " + prefix.key());
				}
				return new Tuple2<>(prefix.key(), codec.decodeKV(new KeyValue(prefix.suffix(), kv.value())));
			}

			@Override
			public Tuple2<K0, K> decodeKey(ByteSequence byteSequence) throws MalformedDataException {
				Prefix<K0> prefix = prefixCodec.decodePrefix(byteSequence);
				EtcdKVCodec<K, T> codec = codecs.apply(prefix.key());
				if (codec == null) {
					throw new MalformedDataException("Unexpected key: " + prefix.key());
				}
				return new Tuple2<>(prefix.key(), codec.decodeKey(prefix.suffix()));
			}

			@Override
			public KeyValue encodeKV(Tuple2<K0, T> entry) {
				KeyValue kv = codecs.apply(entry.value1()).encodeKV(entry.value2());
				Prefix<K0> prefix = new Prefix<>(entry.value1(), kv.key());
				return new KeyValue(prefixCodec.encodePrefix(prefix), kv.value());
			}

			@Override
			public ByteSequence encodeKey(Tuple2<K0, K> key) {
				EtcdKVCodec<K, T> codec = codecs.apply(key.value1());
				Prefix<K0> prefix = new Prefix<>(key.value1(), codec.encodeKey(key.value2()));
				return prefixCodec.encodePrefix(prefix);
			}
		};
	}

	public static <K, T, R> EtcdKVCodec<K, R> transform(EtcdKVCodec<K, T> codec, Function<R, T> encodeFn, DecoderFunction<T, R> decodeFn) {
		return new EtcdKVCodec<>() {
			@Override
			public KeyValue encodeKV(R kv) {
				return codec.encodeKV(encodeFn.apply(kv));
			}

			@Override
			public ByteSequence encodeKey(K key) {
				return codec.encodeKey(key);
			}

			@Override
			public R decodeKV(KeyValue kv) throws MalformedDataException {
				return decodeFn.decode(codec.decodeKV(kv));
			}

			@Override
			public K decodeKey(ByteSequence byteSequence) throws MalformedDataException {
				return codec.decodeKey(byteSequence);
			}
		};
	}

}
