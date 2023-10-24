package io.activej.etcd.codec.kv;

import io.activej.common.exception.MalformedDataException;
import io.activej.common.function.DecoderFunction;
import io.activej.common.tuple.Tuple2;
import io.activej.etcd.codec.key.EtcdKeyCodec;
import io.activej.etcd.codec.prefix.EtcdPrefixCodec;
import io.activej.etcd.codec.prefix.Prefix;
import io.activej.etcd.codec.value.EtcdValueCodec;
import io.activej.etcd.exception.MalformedEtcdDataException;
import io.etcd.jetcd.ByteSequence;

import java.util.Map;
import java.util.function.Function;

public class EtcdKVCodecs {

	public static <V> EtcdKVCodec<Void, V> ofEmptyKey(EtcdValueCodec<V> valueCodec) {
		return new EtcdKVCodec<>() {
			@Override
			public V decodeKV(KeyValue kv) throws MalformedEtcdDataException {
				if (!kv.key().isEmpty()) {
					throw new MalformedEtcdDataException("Detected suffix key: " + kv.key());
				}
				return valueCodec.decodeValue(kv.value());
			}

			@Override
			public Void decodeKey(ByteSequence byteSequence) throws MalformedEtcdDataException {
				if (!byteSequence.isEmpty()) {
					throw new MalformedEtcdDataException("Detected suffix key: " + byteSequence);
				}
				return null;
			}

			@Override
			public KeyValue encodeKV(V v) {
				return new KeyValue(ByteSequence.EMPTY, valueCodec.encodeValue(v));
			}

			@Override
			public ByteSequence encodeKey(Void key) {
				return ByteSequence.EMPTY;
			}
		};
	}

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
			public Map.Entry<K, V> decodeKV(KeyValue kv) throws MalformedEtcdDataException {
				K key = keyCodec.decodeKey(kv.key());
				V value;
				try {
					value = valueCodec.decodeValue(kv.value());
				} catch (MalformedEtcdDataException e) {
					throw new MalformedEtcdDataException("Failed to decode value of key '" + kv.key() + '\'', e);
				}
				return Map.entry(key, value);
			}

			@Override
			public K decodeKey(ByteSequence byteSequence) throws MalformedEtcdDataException {
				return keyCodec.decodeKey(byteSequence);
			}
		};
	}

	public static <K0, K, T> EtcdKVCodec<Tuple2<K0, K>, Tuple2<K0, T>> ofPrefixedEntry(EtcdPrefixCodec<K0> prefixCodec, Function<K0, EtcdKVCodec<K, T>> codecs) {
		return new EtcdKVCodec<>() {
			@Override
			public Tuple2<K0, T> decodeKV(KeyValue kv) throws MalformedEtcdDataException {
				Prefix<K0> prefix = prefixCodec.decodePrefix(kv.key());
				EtcdKVCodec<K, T> codec = codecs.apply(prefix.key());
				if (codec == null) {
					throw new MalformedEtcdDataException("Failed to decode prefixed entry of key '" + kv.key() +
														 "' .Unexpected key: " + prefix.key());
				}
				T t;
				try {
					t = codec.decodeKV(new KeyValue(prefix.suffix(), kv.value()));
				} catch (MalformedEtcdDataException e) {
					throw new MalformedEtcdDataException("Failed to decode KV of key '" + kv.key() + '\'', e);
				}
				return new Tuple2<>(prefix.key(), t);
			}

			@Override
			public Tuple2<K0, K> decodeKey(ByteSequence byteSequence) throws MalformedEtcdDataException {
				Prefix<K0> prefix = prefixCodec.decodePrefix(byteSequence);
				EtcdKVCodec<K, T> codec = codecs.apply(prefix.key());
				if (codec == null) {
					throw new MalformedEtcdDataException("Failed to decode prefixed entry of key '" + byteSequence +
														 "'. Unexpected key: " + prefix.key());
				}
				K k;
				try {
					k = codec.decodeKey(prefix.suffix());
				} catch (MalformedEtcdDataException e) {
					throw new MalformedEtcdDataException("Failed to decode key '" + byteSequence + '\'', e);
				}
				return new Tuple2<>(prefix.key(), k);
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
			public R decodeKV(KeyValue kv) throws MalformedEtcdDataException {
				try {
					return decodeFn.decode(codec.decodeKV(kv));
				} catch (MalformedDataException e) {
					throw new MalformedEtcdDataException("Failed to decode KV of key '" + kv.key() + '\'', e);
				}
			}

			@Override
			public K decodeKey(ByteSequence byteSequence) throws MalformedEtcdDataException {
				return codec.decodeKey(byteSequence);
			}
		};
	}

}
