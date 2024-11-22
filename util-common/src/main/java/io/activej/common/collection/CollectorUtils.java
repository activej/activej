/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.common.collection;

import io.activej.common.Utils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

public final class CollectorUtils {
	public static <K, V, M extends Map<K, V>>
	Collector<Map.Entry<? extends K, ? extends V>, ?, M> entriesToMap(Supplier<M> mapFactory) {
		return toMap(Map.Entry::getKey, Map.Entry::getValue, mapFactory);
	}

	public static <K, V>
	Collector<Map.Entry<? extends K, ? extends V>, ?, LinkedHashMap<K, V>> entriesToLinkedHashMap() {
		return toMap(Map.Entry::getKey, Map.Entry::getValue, LinkedHashMap::new);
	}

	public static <K, V>
	Collector<Map.Entry<? extends K, ? extends V>, ?, HashMap<K, V>> entriesToHashMap() {
		return toMap(Map.Entry::getKey, Map.Entry::getValue, HashMap::new);
	}

	public static <K, V, K1, V1, M extends Map<K1, V1>>
	Collector<Map.Entry<? extends K, ? extends V>, ?, M> entriesToMap(
		Function<? super K, ? extends K1> keyMapper,
		Function<? super V, ? extends V1> valueMapper,
		Supplier<M> mapFactory
	) {
		return toMap(entry -> keyMapper.apply(entry.getKey()), entry -> valueMapper.apply(entry.getValue()), mapFactory);
	}

	public static <K, V, K1, V1>
	Collector<Map.Entry<? extends K, ? extends V>, ?, LinkedHashMap<K1, V1>> entriesToLinkedHashMap(
		Function<? super K, ? extends K1> keyMapper,
		Function<? super V, ? extends V1> valueMapper
	) {
		return entriesToMap(keyMapper, valueMapper, LinkedHashMap::new);
	}

	public static <K, V, K1, V1>
	Collector<Map.Entry<? extends K, ? extends V>, ?, HashMap<K1, V1>> entriesToHashMap(
		Function<? super K, ? extends K1> keyMapper,
		Function<? super V, ? extends V1> valueMapper
	) {
		return entriesToMap(keyMapper, valueMapper, HashMap::new);
	}

	public static <K, V, V1, M extends Map<K, V1>>
	Collector<Map.Entry<? extends K, ? extends V>, ?, M> entriesToMap(
		Function<? super V, ? extends V1> valueMapper,
		Supplier<M> mapFactory
	) {
		return toMap(Map.Entry::getKey, entry -> valueMapper.apply(entry.getValue()), mapFactory);
	}

	public static <K, V, V1>
	Collector<Map.Entry<? extends K, ? extends V>, ?, LinkedHashMap<K, V1>> entriesToLinkedHashMap(
		Function<? super V, ? extends V1> valueMapper
	) {
		return entriesToMap(valueMapper, LinkedHashMap::new);
	}

	public static <K, V, V1>
	Collector<Map.Entry<? extends K, ? extends V>, ?, HashMap<K, V1>> entriesToHashMap(
		Function<? super V, ? extends V1> valueMapper
	) {
		return entriesToMap(valueMapper, HashMap::new);
	}

	public static <T, K, V, M extends Map<K, V>>
	Collector<T, ?, M> toMap(
		Function<? super T, ? extends K> keyMapper,
		Function<? super T, ? extends V> valueMapper,
		Supplier<M> mapFactory
	) {
		return Collectors.toMap(keyMapper, valueMapper, Utils.noMergeFunction(), mapFactory);
	}

	public static <T, K, V>
	Collector<T, ?, LinkedHashMap<K, V>> toLinkedHashMap(
		Function<? super T, ? extends K> keyMapper,
		Function<? super T, ? extends V> valueMapper
	) {
		return toMap(keyMapper, valueMapper, LinkedHashMap::new);
	}

	public static <T, K, V>
	Collector<T, ?, HashMap<K, V>> toHashMap(
		Function<? super T, ? extends K> keyMapper,
		Function<? super T, ? extends V> valueMapper
	) {
		return toMap(keyMapper, valueMapper, HashMap::new);
	}

	public static <K, V, M extends Map<K, V>>
	Collector<K, ?, M> toMap(
		Function<? super K, ? extends V> valueMapper,
		Supplier<M> mapFactory
	) {
		return toMap(identity(), valueMapper, mapFactory);
	}

	public static <K, V>
	Collector<K, ?, LinkedHashMap<K, V>> toLinkedHashMap(
		Function<? super K, ? extends V> valueMapper
	) {
		return toLinkedHashMap(identity(), valueMapper);
	}

	public static <K, V>
	Collector<K, ?, HashMap<K, V>> toHashMap(
		Function<? super K, ? extends V> valueMapper
	) {
		return toHashMap(identity(), valueMapper);
	}
}
