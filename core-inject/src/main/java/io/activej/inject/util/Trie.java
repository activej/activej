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

package io.activej.inject.util;

import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.activej.inject.util.Utils.next;
import static java.util.Collections.emptyMap;

/**
 * Completely generic and abstract simple Java implementation
 * of the <a href="https://en.wikipedia.org/wiki/Trie">prefixed tree (or trie)</a> data structure.
 */
public final class Trie<K, V> {
	private final V payload;
	private final Map<K, Trie<K, V>> children;

	public Trie(V payload, Map<K, Trie<K, V>> children) {
		this.payload = payload;
		this.children = children;
	}

	public static <K, V> Trie<K, V> leaf(V value) {
		return new Trie<>(value, new HashMap<>());
	}

	public static <K, V> Trie<K, V> of(V payload, Map<K, Trie<K, V>> children) {
		return new Trie<>(payload, children);
	}

	public V get() {
		return payload;
	}

	public Map<K, Trie<K, V>> getChildren() {
		return children;
	}

	public Trie<K, V> get(K key) {
		return children.get(key);
	}

	public Trie<K, V> getOrDefault(K key, V defaultValue) {
		return children.getOrDefault(key, new Trie<>(defaultValue, emptyMap()));
	}

	public Trie<K, V> computeIfAbsent(K key, Function<K, V> f) {
		return children.computeIfAbsent(key, k -> leaf(f.apply(k)));
	}

	public @Nullable Trie<K, V> get(K[] path) {
		Trie<K, V> subtree = this;
		for (K key : path) {
			subtree = subtree.get(key);
			if (subtree == null) {
				return null;
			}
		}
		return subtree;
	}

	public Trie<K, V> computeIfAbsent(K[] path, Function<K, V> f) {
		Trie<K, V> subtree = this;
		for (K key : path) {
			subtree = subtree.computeIfAbsent(key, f);
		}
		return subtree;
	}

	public void addAll(Trie<K, V> other, BiConsumer<V, V> merger) {
		mergeInto(this, other, merger);
	}

	public <E> Trie<K, E> map(Function<? super V, ? extends E> fn) {
		Trie<K, E> root = leaf(fn.apply(payload));
		children.forEach((k, sub) -> root.children.put(k, sub.map(fn)));
		return root;
	}

	public void dfs(K[] path, BiConsumer<K[], V> consumer) {
		Trie<K, V> sub = get(path);
		if (sub != null) {
			sub.dfsImpl(path, consumer);
		}
	}

	private void dfsImpl(K[] path, BiConsumer<K[], V> consumer) {
		children.forEach((key, child) -> child.dfsImpl(next(path, key), consumer));
		consumer.accept(path, payload);
	}

	public void dfs(Consumer<V> consumer) {
		children.forEach((key, child) -> child.dfs(consumer));
		consumer.accept(payload);
	}

	private static <K, V> void mergeInto(Trie<K, V> into, Trie<K, V> from, BiConsumer<V, V> merger) {
		if (into == from) {
			return;
		}
		merger.accept(into.get(), from.get());
		from.children.forEach((scope, child) -> mergeInto(into.children.computeIfAbsent(scope, $ -> child), child, merger));
	}

	public static <K, V> Trie<K, V> merge(BiConsumer<V, V> merger, V rootPayload, Trie<K, V> first, Trie<K, V> second) {
		Trie<K, V> combined = leaf(rootPayload);
		mergeInto(combined, first, merger);
		mergeInto(combined, second, merger);
		return combined;
	}

	@SafeVarargs
	public static <K, V> Trie<K, V> merge(BiConsumer<V, V> merger, V rootPayload, Trie<K, V> first, Trie<K, V> second, Trie<K, V>... rest) {
		return merge(merger, rootPayload, Stream.concat(Stream.of(first, second), Arrays.stream(rest)));
	}

	public static <K, V> Trie<K, V> merge(BiConsumer<V, V> merger, V rootPayload, Collection<Trie<K, V>> bindings) {
		return merge(merger, rootPayload, bindings.stream());
	}

	public static <K, V> Trie<K, V> merge(BiConsumer<V, V> merger, V rootPayload, Stream<Trie<K, V>> bindings) {
		Trie<K, V> combined = leaf(rootPayload);
		bindings.forEach(sb -> mergeInto(combined, sb, merger));
		return combined;
	}

	public String prettyPrint() {
		return prettyPrint(0);
	}

	private String prettyPrint(int indent) {
		String indentStr = new String(new char[indent]).replace('\0', '\t');

		StringBuilder sb = new StringBuilder()
				.append("(")
				.append(payload)
				.append(") {");

		if (!children.isEmpty()) {
			sb.append('\n').append(indentStr);
			children.forEach((key, child) -> sb
					.append(indentStr)
					.append('\t')
					.append(key)
					.append(" -> ")
					.append(child.prettyPrint(indent + 1)));
		}
		return sb.append("}\n").toString();
	}

	@Override
	public String toString() {
		return "{" + payload + ", " + children + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Trie<?, ?> trie = (Trie<?, ?>) o;
		return Objects.equals(payload, trie.payload) && children.equals(trie.children);
	}

	@Override
	public int hashCode() {
		int result = payload != null ? payload.hashCode() : 0;
		result = 31 * result + children.hashCode();
		return result;
	}
}
