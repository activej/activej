package io.activej.serializer.stream;

import io.activej.common.collection.CollectionUtils;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.IntFunction;

import static io.activej.common.collection.CollectionUtils.newHashMap;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class SizedCollectorsKV {

	public static <K, V, M extends Map<K, V>> SizedCollectorKV<K, V, ?, M> toMap(IntFunction<? extends M> factory) {
		return new SizedCollectorKV<K, V, M, M>() {
			@Override
			public M accumulator(int size) {
				return factory.apply(size);
			}

			@Override
			public void accumulate(M map, int index, K key, V value) {
				map.put(key, value);
			}

			@Override
			public M result(M map) {
				return map;
			}
		};
	}

	public static <K, V> SizedCollectorKV<K, V, ?, Map<K, V>> toMap() {
		return new SizedCollectorKV<K, V, HashMap<K, V>, Map<K, V>>() {
			@Override
			public Map<K, V> create0() {
				return emptyMap();
			}

			@Override
			public Map<K, V> create1(K key, V value) {
				return singletonMap(key, value);
			}

			@Override
			public HashMap<K, V> accumulator(int size) {
				return newHashMap(size);
			}

			@Override
			public void accumulate(HashMap<K, V> accumulator, int index, K key, V value) {
				accumulator.put(key, value);
			}

			@Override
			public Map<K, V> result(HashMap<K, V> accumulator) {
				return accumulator;
			}
		};
	}

	public static <K, V> SizedCollectorKV<K, V, ?, HashMap<K, V>> toHashMap() {
		return toMap(CollectionUtils::newHashMap);
	}

	public static <K, V> SizedCollectorKV<K, V, ?, LinkedHashMap<K, V>> toLinkedHashMap() {
		return toMap(CollectionUtils::newLinkedHashMap);
	}

	public static <K extends Enum<K>, V> SizedCollectorKV<K, V, ?, EnumMap<K, V>> toEnumMap(Class<K> type) {
		return toMap(size -> new EnumMap<>(type));
	}

}
