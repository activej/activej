package io.activej.serializer.stream;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

class SizedCollectorsKV {
	public static <K, V> SizedCollectorKV<K, V, HashMap<K, V>, Map<K, V>> toMap() {
		return new SizedCollectorKV<>() {
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
				return new HashMap<>(hashInitialSize(size));
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

	public static <K, V> SizedCollectorKV<K, V, HashMap<K, V>, HashMap<K, V>> toHashMap() {
		return SizedCollectorKV.toMap(size -> new HashMap<>(hashInitialSize(size)));
	}

	public static <K, V> SizedCollectorKV<K, V, LinkedHashMap<K, V>, LinkedHashMap<K, V>> toLinkedHashMap() {
		return SizedCollectorKV.toMap(size -> new LinkedHashMap<>(hashInitialSize(size)));
	}

	static int hashInitialSize(int length) {
		return (length + 2) / 3 * 4;
	}

	public static <K extends Enum<K>, V> SizedCollectorKV<K, V, EnumMap<K, V>, EnumMap<K, V>> toEnumMap(Class<K> type) {
		return SizedCollectorKV.toMap(size -> new EnumMap<>(type));
	}
}
