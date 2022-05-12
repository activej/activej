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

import io.activej.inject.InstanceProvider;
import io.activej.inject.Key;
import io.activej.inject.KeyPattern;
import io.activej.inject.Scope;
import io.activej.inject.binding.Binding;
import io.activej.inject.binding.BindingType;
import io.activej.inject.binding.DIException;
import io.activej.inject.binding.Multibinder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;

import static io.activej.inject.Scope.UNSCOPED;
import static io.activej.inject.binding.BindingType.*;
import static io.activej.types.IsAssignableUtils.isAssignable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public final class Utils {

	private static final BiConsumer<Map<Key<?>, Set<Binding<?>>>, Map<Key<?>, Set<Binding<?>>>> BINDING_MULTIMAP_MERGER =
			(into, from) -> from.forEach((key, v) -> into.merge(key, v, (set1, set2) -> {
				Set<Binding<?>> set = new HashSet<>(set1.size() + set2.size());
				set.addAll(set1);
				set.addAll(set2);
				BindingType type = set.isEmpty() ? null : set.iterator().next().getType();
				if (set.stream().anyMatch(b -> b.getType() != type)) {
					throw new DIException("Two binding sets bound with different types for key " + key.getDisplayString());
				}
				return set;
			}));

	public static BiConsumer<Map<Key<?>, Set<Binding<?>>>, Map<Key<?>, Set<Binding<?>>>> bindingMultimapMerger() {
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
		if (first.isEmpty()) return second;
		if (second.isEmpty()) return first;
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
		return toMap(keyMapper, t -> Set.of(valueMapper.apply(t)), Utils::union);
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
	public static void printGraphVizGraph(Trie<Scope, Map<Key<?>, Binding<?>>> trie) {
		System.out.println(makeGraphVizGraph(trie));
	}

	/**
	 * Makes a GraphViz graph representation of the binding graph.
	 * Scopes are grouped nicely into subgraph boxes and dependencies are properly drawn from lower to upper scopes.
	 */
	public static String makeGraphVizGraph(Trie<Scope, Map<Key<?>, Binding<?>>> trie) {
		StringBuilder sb = new StringBuilder();
		sb.append("digraph {\n\trankdir=BT;\n");
		Set<ScopedKey> known = new HashSet<>();
		writeNodes(UNSCOPED, trie, known, "", new int[]{0}, sb);
		writeEdges(UNSCOPED, trie, known, sb);
		sb.append("}\n");
		return sb.toString();
	}

	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	private static void writeNodes(Scope[] scope, Trie<Scope, Map<Key<?>, Binding<?>>> trie, Set<ScopedKey> known, String indent, int[] scopeCount, StringBuilder sb) {
		if (scope != UNSCOPED) {
			sb.append("\n" + indent)
					.append("subgraph cluster_" + (scopeCount[0]++) + " {\n")
					.append(indent + "\tlabel=\"" + scope[scope.length - 1].getDisplayString().replace("\"", "\\\"") + "\"\n");
		}

		for (Entry<Scope, Trie<Scope, Map<Key<?>, Binding<?>>>> entry : trie.getChildren().entrySet()) {
			writeNodes(next(scope, entry.getKey()), entry.getValue(), known, indent + '\t', scopeCount, sb);
		}

		Set<Key<?>> leafs = new HashSet<>();

		for (Entry<Key<?>, Binding<?>> entry : trie.get().entrySet()) {
			Key<?> key = entry.getKey();
			Binding<?> bindingInfo = entry.getValue();

			if (bindingInfo.getDependencies().isEmpty()) {
				leafs.add(key);
			}
			known.add(ScopedKey.of(scope, key));
			sb.append(indent)
					.append('\t')
					.append('"' + getScopeId(scope) + key.toString().replace("\"", "\\\"") + '"')
					.append(" [label=" +
							'"' + key.getDisplayString().replace("\"", "\\\"") + '"')
					.append(
							bindingInfo.getType() == TRANSIENT ? " style=dotted" :
									bindingInfo.getType() == EAGER ? " style=bold" :
											bindingInfo.getType() == SYNTHETIC ? " style=dashed" :
													"")
					.append("];")
					.append('\n');
		}

		if (!leafs.isEmpty()) {
			sb.append(leafs.stream()
					.map(key -> '"' + getScopeId(scope) + key.toString().replace("\"", "\\\"") + '"')
					.collect(joining(" ",
							'\n' + indent + '\t' + "{ rank=same; ",
							" }\n")));
			if (scope == UNSCOPED) {
				sb.append('\n');
			}
		}

		if (scope != UNSCOPED) {
			sb.append(indent + "}\n\n");
		}
	}

	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	private static void writeEdges(Scope[] scope, Trie<Scope, Map<Key<?>, Binding<?>>> trie, Set<ScopedKey> known, StringBuilder sb) {
		String scopePath = getScopeId(scope);

		for (Entry<Key<?>, Binding<?>> entry : trie.get().entrySet()) {
			String key = "\"" + scopePath + entry.getKey().toString().replace("\"", "\\\"") + "\"";
			for (Key<?> dependency : entry.getValue().getDependencies()) {
				Scope[] depScope = scope;
				while (!known.contains(ScopedKey.of(depScope, dependency)) && depScope.length != 0) {
					depScope = Arrays.copyOfRange(depScope, 0, depScope.length - 1);
				}

				String dep = '"' + getScopeId(depScope) + dependency.toString().replace("\"", "\\\"") + '"';
				if (depScope.length == 0) {
					if (known.add(ScopedKey.of(depScope, dependency))) {
						sb.append('\t')
								.append(dep)
								.append(" [label=" +
										'"' + dependency.getDisplayString().replace("\"", "\\\"") + '"')
								.append(" style=dashed, color=red];")
								.append('\n');
					}
				}
				sb.append('\t' + key + " -> " + dep);
				sb.append(" [");
				if (dependency.getRawType() == InstanceProvider.class) {
					sb.append("color=gray");
				}
				sb.append("];\n");
			}
		}
		for (Entry<Scope, Trie<Scope, Map<Key<?>, Binding<?>>>> entry : trie.getChildren().entrySet()) {
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

	public static @NotNull String getDisplayString(@NotNull Class<? extends Annotation> annotationType, @Nullable Annotation annotation) {
		if (annotation == null) {
			return "@" + ReflectionUtils.getDisplayName(annotationType);
		}
		String typeName = annotationType.getName();
		String str = annotation.toString();
		return str.startsWith("@" + typeName) ? "@" + ReflectionUtils.getDisplayName(annotationType) + str.substring(typeName.length() + 1) : str;
	}

	public static String getDisplayString(@NotNull Object object) {
		if (object instanceof Class && ((Class<?>) object).isAnnotation()) {
			//noinspection unchecked
			return getDisplayString((Class<? extends Annotation>) object, null);
		}
		if (object instanceof Annotation annotation) {
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

	public static <T> LinkedHashMap<KeyPattern<?>, Set<T>> sortPatternsMap(Map<KeyPattern<?>, Set<T>> map) {
		return map.entrySet().stream()
				.sorted((entry1, entry2) -> {
					KeyPattern<?> pattern1 = entry1.getKey();
					KeyPattern<?> pattern2 = entry2.getKey();
					Type type1 = pattern1.getType();
					Type type2 = pattern2.getType();
					if (type1.equals(type2)) {
						if (!pattern1.hasQualifier() && pattern2.hasQualifier()) return 1;
						if (pattern1.hasQualifier() && !pattern2.hasQualifier()) return -1;
						return Integer.compare(System.identityHashCode(type1), System.identityHashCode(type2));
					}
					if (isAssignable(type1, type2)) return 1;
					if (isAssignable(type2, type1)) return -1;
					return Integer.compare(System.identityHashCode(type1), System.identityHashCode(type2));
				})
				.collect(toMap(Entry::getKey, Entry::getValue,
						(v1, v2) -> {throw new AssertionError();}, LinkedHashMap::new));
	}

	public static @Nullable Type match(Type type, Collection<Type> patterns) {
		Type best = null;
		for (Type found : patterns) {
			if (isAssignable(found, type)) {
				if (best == null || isAssignable(best, found)) {
					if (best != null && !best.equals(found) && isAssignable(found, best)) {
						throw new IllegalArgumentException("Conflicting types: " + type + " " + best);
					}
					best = found;
				}
			}
		}
		return best;
	}
}
