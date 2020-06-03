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

package io.activej.di.util;

import io.activej.di.Key;
import io.activej.di.Scope;
import io.activej.di.binding.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;

import static io.activej.di.Scope.UNSCOPED;
import static io.activej.di.binding.BindingType.EAGER;
import static io.activej.di.binding.BindingType.TRANSIENT;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.*;

public final class Utils {

	private static final BiConsumer<Map<Key<?>, BindingSet<?>>, Map<Key<?>, BindingSet<?>>> BINDING_MULTIMAP_MERGER =
			(into, from) -> from.forEach((k, v) -> into.merge(k, v, (first, second) -> BindingSet.merge(k, first, second)));

	public static BiConsumer<Map<Key<?>, BindingSet<?>>, Map<Key<?>, BindingSet<?>>> bindingMultimapMerger() {
		return BINDING_MULTIMAP_MERGER;
	}

	public static <T> T[] next(T[] items, T item) {
		T[] next = Arrays.copyOf(items, items.length + 1);
		next[items.length] = item;
		return next;
	}

	public static String getScopeDisplayString(Scope[] scope) {
		return Arrays.stream(scope).map(Scope::getDisplayString).collect(joining("->", "()", ""));
	}

	public static void mergeMultibinders(Map<Key<?>, Multibinder<?>> into, Map<Key<?>, Multibinder<?>> from) {
		from.forEach((k, v) -> into.merge(k, v, (oldResolver, newResolver) -> {
			if (!oldResolver.equals(newResolver)) {
				throw new DIException("More than one multibinder per key");
			}
			return oldResolver;
		}));
	}

	public static <K, V> void combineMultimap(Map<K, Set<V>> accumulator, Map<K, Set<V>> multimap) {
		multimap.forEach((key, set) -> accumulator.computeIfAbsent(key, $ -> new HashSet<>()).addAll(set));
	}

	public static <T> Set<T> union(Set<T> first, Set<T> second) {
		Set<T> result = new HashSet<>((first.size() + second.size()) * 4 / 3 + 1);
		result.addAll(first);
		result.addAll(second);
		return result;
	}

	public static <K, V> Map<K, V> override(Map<K, V> into, Map<K, V> from) {
		Map<K, V> result = new HashMap<>((from.size() + into.size()) * 4 / 3 + 1);
		result.putAll(from);
		result.putAll(into);
		return result;
	}

	public static <T, K, V> Collector<T, ?, Map<K, Set<V>>> toMultimap(Function<? super T, ? extends K> keyMapper,
			Function<? super T, ? extends V> valueMapper) {
		return toMap(keyMapper, t -> singleton(valueMapper.apply(t)), Utils::union);
	}

	public static <K, V, V1> Map<K, Set<V1>> transformMultimapValues(Map<K, Set<V>> multimap, BiFunction<? super K, ? super V, ? extends V1> fn) {
		return transformMultimap(multimap, Function.identity(), fn);
	}

	public static <K, V, K1, V1> Map<K1, Set<V1>> transformMultimap(Map<K, Set<V>> multimap, Function<? super K, ? extends K1> fnKey, BiFunction<? super K, ? super V, ? extends V1> fnValue) {
		return multimap.entrySet()
				.stream()
				.collect(toMap(
						entry -> fnKey.apply(entry.getKey()),
						entry -> entry.getValue()
								.stream()
								.map(v -> fnValue.apply(entry.getKey(), v))
								.collect(toSet())));
	}

	public static Map<Key<?>, BindingSet<?>> transformBindingMultimapValues(Map<Key<?>, BindingSet<?>> multimap, BiFunction<Key<?>, Binding<?>, Binding<?>> fn) {
		return transformBindingMultimap(multimap, UnaryOperator.identity(), fn);
	}

	public static Map<Key<?>, BindingSet<?>> transformBindingMultimap(Map<Key<?>, BindingSet<?>> multimap, UnaryOperator<Key<?>> fnKey, BiFunction<Key<?>, Binding<?>, Binding<?>> fnValue) {
		return multimap.entrySet()
				.stream()
				.collect(toMap(
						entry -> fnKey.apply(entry.getKey()),
						entry -> {
							BindingSet<?> bindingSet = entry.getValue();
							return new BindingSet<>(
									bindingSet
											.getBindings()
											.stream()
											.map(v -> fnValue.apply(entry.getKey(), v))
											.collect(toSet()),
									bindingSet.getType());
						}));
	}

	public static <K, V> Map<K, V> squash(Map<K, Set<V>> multimap, BiFunction<K, Set<V>, V> squasher) {
		return multimap.entrySet().stream()
				.collect(toMap(Entry::getKey, e -> squasher.apply(e.getKey(), e.getValue())));
	}

	public static void checkArgument(boolean condition, String message) {
		if (!condition) {
			throw new IllegalArgumentException(message);
		}
	}

	public static void checkState(boolean condition, String message) {
		if (!condition) {
			throw new IllegalStateException(message);
		}
	}

	public static String getLocation(@Nullable Binding<?> binding) {
		LocationInfo location = binding != null ? binding.getLocation() : null;
		return "at " + (location != null ? location.toString() : "<unknown binding location>");
	}

	/**
	 * A shortcut for printing the result of {@link #makeGraphVizGraph} into the standard output.
	 */
	public static void printGraphVizGraph(Trie<Scope, Map<Key<?>, BindingInfo>> trie) {
		System.out.println(makeGraphVizGraph(trie));
	}

	/**
	 * Makes a GraphViz graph representation of the binding graph.
	 * Scopes are grouped nicely into subgraph boxes and dependencies are properly drawn from lower to upper scopes.
	 */
	public static String makeGraphVizGraph(Trie<Scope, Map<Key<?>, BindingInfo>> trie) {
		StringBuilder sb = new StringBuilder();
		sb.append("digraph {\n	rankdir=BT;\n");
		Set<ScopedValue<Key<?>>> known = new HashSet<>();
		writeNodes(UNSCOPED, trie, known, "", new int[]{0}, sb);
		writeEdges(UNSCOPED, trie, known, sb);
		sb.append("}\n");
		return sb.toString();
	}

	private static void writeNodes(Scope[] scope, Trie<Scope, Map<Key<?>, BindingInfo>> trie, Set<ScopedValue<Key<?>>> known, String indent, int[] scopeCount, StringBuilder sb) {
		if (scope != UNSCOPED) {
			sb.append('\n').append(indent)
					.append("subgraph cluster_").append(scopeCount[0]++).append(" {\n")
					.append(indent).append("\tlabel=\"").append(scope[scope.length - 1].getDisplayString().replace("\"", "\\\"")).append("\"\n");
		}

		for (Entry<Scope, Trie<Scope, Map<Key<?>, BindingInfo>>> entry : trie.getChildren().entrySet()) {
			writeNodes(next(scope, entry.getKey()), entry.getValue(), known, indent + '\t', scopeCount, sb);
		}

		Set<Key<?>> leafs = new HashSet<>();

		for (Entry<Key<?>, BindingInfo> entry : trie.get().entrySet()) {
			Key<?> key = entry.getKey();
			BindingInfo bindingInfo = entry.getValue();

			if (bindingInfo.getDependencies().isEmpty()) {
				leafs.add(key);
			}
			known.add(ScopedValue.of(scope, key));
			sb.append(indent)
					.append('\t')
					.append('"').append(getScopeId(scope)).append(key.toString().replace("\"", "\\\"")).append('"')
					.append(" [label=\"").append(key.getDisplayString().replace("\"", "\\\""))
					.append("\"")
					.append(bindingInfo.getType() == TRANSIENT ? " style=dotted" : bindingInfo.getType() == EAGER ? " style=bold" : "")
					.append("];\n");
		}

		if (!leafs.isEmpty()) {
			sb.append(leafs.stream()
					.map(key -> '"' + getScopeId(scope) + key.toString().replace("\"", "\\\"") + '"')
					.collect(joining(" ", '\n' + indent + "\t{ rank=same; ", " }\n")));
			if (scope == UNSCOPED) {
				sb.append('\n');
			}
		}

		if (scope != UNSCOPED) {
			sb.append(indent).append("}\n\n");
		}
	}

	private static void writeEdges(Scope[] scope, Trie<Scope, Map<Key<?>, BindingInfo>> trie, Set<ScopedValue<Key<?>>> known, StringBuilder sb) {
		String scopePath = getScopeId(scope);

		for (Entry<Key<?>, BindingInfo> entry : trie.get().entrySet()) {
			String key = "\"" + scopePath + entry.getKey().toString().replace("\"", "\\\"") + "\"";
			for (Dependency dependency : entry.getValue().getDependencies()) {
				Key<?> depKey = dependency.getKey();

				Scope[] depScope = scope;
				while (!known.contains(ScopedValue.of(depScope, depKey)) && depScope.length != 0) {
					depScope = Arrays.copyOfRange(depScope, 0, depScope.length - 1);
				}

				if (depScope.length == 0) {
					String dep = "\"" + getScopeId(depScope) + depKey.toString().replace("\"", "\\\"") + '"';

					if (known.add(ScopedValue.of(depScope, depKey))) {
						sb.append('\t')
								.append(dep)
								.append(" [label=\"")
								.append(depKey.getDisplayString().replace("\"", "\\\""))
								.append("\" style=dashed, color=")
								.append(dependency.isRequired() ? "red" : "orange")
								.append("];\n");
					}
					sb.append('\t').append(key).append(" -> ").append(dep);
				} else {
					sb.append('\t').append(key).append(" -> \"").append(getScopeId(depScope)).append(depKey.toString().replace("\"", "\\\"")).append('"');
				}
				sb.append(" [");
				if (!dependency.isRequired()) {
					sb.append("style=dashed,");
				}
				if (dependency.isImplicit()) {
					sb.append("color=gray");
				}
				sb.append("];\n");
			}
		}
		for (Entry<Scope, Trie<Scope, Map<Key<?>, BindingInfo>>> entry : trie.getChildren().entrySet()) {
			writeEdges(next(scope, entry.getKey()), entry.getValue(), known, sb);
		}
	}

	private static String getScopeId(Scope[] scope) {
		return Arrays.stream(scope).map(Scope::toString).collect(joining("->", "()->", "")).replace("\"", "\\\"");
	}

	public static int getKeyDisplayCenter(Key<?> key) {
		Object qualifier = key.getQualifier();
		int nameOffset = qualifier != null ? getDisplayString(qualifier).length() + 1 : 0;
		return nameOffset + (key.getDisplayString().length() - nameOffset) / 2;
	}

	@NotNull
	public static String getDisplayString(@NotNull Class<? extends Annotation> annotationType, @Nullable Annotation annotation) {
		if (annotation == null) {
			return "@" + ReflectionUtils.getDisplayName(annotationType);
		}
		String typeName = annotationType.getName();
		String str = annotation.toString();
		return str.startsWith("@" + typeName) ? "@" + ReflectionUtils.getDisplayName(annotationType) + str.substring(typeName.length() + 1) : str;
	}

	public static String getDisplayString(@NotNull Object object){
		if (object instanceof Class && ((Class<?>) object).isAnnotation()){
			//noinspection unchecked
			return getDisplayString((Class<? extends Annotation>) object, null);
		}
		if (object instanceof Annotation){
			Annotation annotation = (Annotation) object;
			return getDisplayString(annotation.annotationType(), annotation);
		}
		return object.toString();
	}

	public static String drawCycle(Key<?>[] cycle) {
		int offset = getKeyDisplayCenter(cycle[0]);
		String cycleString = Arrays.stream(cycle).map(Key::getDisplayString).collect(joining(" -> ", "\t", ""));
		String indent = new String(new char[offset]).replace('\0', ' ');
		String line = new String(new char[cycleString.length() - offset]).replace('\0', '-');
		return cycleString + " -,\n\t" + indent + "^" + line + "'";
	}

	public static boolean isMarker(Class<? extends Annotation> annotationType) {
		return annotationType.getDeclaredMethods().length == 0;
	}
}
