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

package io.activej.inject.module;

import io.activej.inject.Key;
import io.activej.inject.KeyPattern;
import io.activej.inject.Scope;
import io.activej.inject.binding.Binding;
import io.activej.inject.binding.BindingGenerator;
import io.activej.inject.binding.BindingTransformer;
import io.activej.inject.binding.Multibinder;
import io.activej.inject.impl.CompiledBinding;
import io.activej.inject.impl.CompiledBindingLocator;
import io.activej.inject.util.Trie;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static io.activej.inject.Qualifiers.uniqueQualifier;
import static io.activej.inject.Scope.UNSCOPED;
import static io.activej.inject.util.Utils.*;
import static java.util.stream.Collectors.toCollection;

/**
 * This class contains a set of utilities for working with {@link Module modules}.
 */
public final class Modules {
	static final Module EMPTY = new SimpleModule(Trie.leaf(Map.of()), Map.of(), Map.of(), Map.of());

	/**
	 * Combines multiple modules into one.
	 */
	public static Module combine(Collection<Module> modules) {
		if (modules.size() == 1) {
			return modules.iterator().next();
		}
		Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.merge(bindingMultimapMerger(), new HashMap<>(), modules.stream().map(Module::getBindings));

		Map<KeyPattern<?>, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>();
		Map<KeyPattern<?>, Set<BindingTransformer<?>>> bindingTransformers = new HashMap<>();
		Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>();

		for (Module module : modules) {
			combineMultimap(bindingTransformers, module.getBindingTransformers());
			combineMultimap(bindingGenerators, module.getBindingGenerators());
			mergeMultibinders(multibinders, module.getMultibinders());
		}

		return new SimpleModule(bindings, bindingTransformers, bindingGenerators, multibinders);
	}

	/**
	 * Combines multiple modules into one.
	 *
	 * @see #combine(Collection)
	 */
	public static Module combine(Module... modules) {
		return modules.length == 0 ? Module.empty() : modules.length == 1 ? modules[0] : combine(List.of(modules));
	}

	/**
	 * Consecutively overrides each of the given modules with the next one after it and returns the accumulated result.
	 */
	public static Module override(List<Module> modules) {
		return modules.stream().reduce(Module.empty(), Modules::override);
	}

	/**
	 * Consecutively overrides each of the given modules with the next one after it and returns the accumulated result.
	 *
	 * @see #override(List) (Collection)
	 */
	public static Module override(Module... modules) {
		return override(List.of(modules));
	}

	/**
	 * This method creates a module that has bindings, transformers, generators and multibinders from first module
	 * replaced with bindings, transformers, generators and multibinders from the second module.
	 */
	public static Module override(Module into, Module replacements) {
		Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.merge(Map::putAll, new HashMap<>(), into.getBindings(), replacements.getBindings());

		Map<KeyPattern<?>, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>(into.getBindingGenerators());
		bindingGenerators.putAll(replacements.getBindingGenerators());

		Map<KeyPattern<?>, Set<BindingTransformer<?>>> bindingTransformers = new HashMap<>(into.getBindingTransformers());
		bindingTransformers.putAll(replacements.getBindingTransformers());

		Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>(into.getMultibinders());
		multibinders.putAll(replacements.getMultibinders());

		return new SimpleModule(bindings, bindingTransformers, bindingGenerators, multibinders);
	}

	/**
	 * Creates a module with all trie nodes merged into one and placed at root.
	 * Basically, any scopes are ignored.
	 * This is useful for some tests.
	 */
	public static Module ignoreScopes(Module from) {
		Map<Key<?>, Set<Binding<?>>> bindings = new HashMap<>();
		Map<Key<?>, Scope[]> scopes = new HashMap<>();
		from.getBindings().dfs(UNSCOPED, (scope, localBindings) ->
				localBindings.forEach((k, b) -> {
					bindings.merge(k, b, ($, $2) -> {
						Scope[] alreadyThere = scopes.get(k);
						String where = alreadyThere.length == 0 ? "in root" : "in scope " + getScopeDisplayString(alreadyThere);
						throw new IllegalStateException("Duplicate key " + k + ", already defined " + where + " and in scope " + getScopeDisplayString(scope));
					});
					scopes.put(k, scope);
				}));
		return new SimpleModule(Trie.leaf(bindings), from.getBindingTransformers(), from.getBindingGenerators(), from.getMultibinders());
	}

	public static Trie<Scope, Set<Key<?>>> getImports(Trie<Scope, Map<Key<?>, Set<Binding<?>>>> trie) {
		return getImports(trie, Set.of());
	}

	private static Trie<Scope, Set<Key<?>>> getImports(Trie<Scope, Map<Key<?>, Set<Binding<?>>>> trie, Set<Key<?>> upperExports) {
		Set<Key<?>> exports = union(upperExports, trie.get().keySet());
		Set<Key<?>> imports = trie.get().values().stream()
				.flatMap(bindings -> bindings.stream()
						.flatMap(binding -> binding.getDependencies().stream()))
				.collect(toCollection(HashSet::new));
		imports.removeAll(exports);
		Map<Scope, Trie<Scope, Set<Key<?>>>> subMap = new HashMap<>();
		trie.getChildren().forEach((key, subtrie) -> subMap.put(key, getImports(subtrie, exports)));
		return Trie.of(imports, subMap);
	}

	public static Module remap(Module module, Map<Key<?>, Key<?>> remapping) {
		Map<Key<?>, Key<?>> remappingInverted = new HashMap<>(remapping.size());
		remapping.forEach((key1, key2) -> remappingInverted.put(key2, key1));
		checkArgument(remappingInverted.size() == remapping.size(), "Duplicate keys");
		return remap(module, key -> remappingInverted.getOrDefault(key, key));
	}

	public static Module remap(Module module, UnaryOperator<Key<?>> remapping) {
		return remap(module, (path, key) -> remapping.apply(key), (path, key) -> remapping.apply(key));
	}

	public static Module remap(Module module,
			BiFunction<Scope[], Key<?>, Key<?>> exportsMapping,
			BiFunction<Scope[], Key<?>, Key<?>> importsMapping) {
		return new SimpleModule(
				remap(exportsMapping, importsMapping, UNSCOPED, module.getBindings(), Map.of()),
				module.getBindingTransformers(),
				module.getBindingGenerators(),
				module.getMultibinders());
	}

	public static Module restrict(Module module, Key<?>... exports) {
		return restrict(module, Set.of(exports));
	}

	public static Module restrict(Module module, Set<Key<?>> exports) {
		return restrict(module, exports::contains);
	}

	public static Module restrict(Module module, Predicate<Key<?>> exportsPredicate) {
		return restrict(module, (scopes, key) -> exportsPredicate.test(key));
	}

	public static Module restrict(Module module, BiPredicate<Scope[], Key<?>> exportsPredicate) {
		return new SimpleModule(
				restrict(exportsPredicate, module.getBindings()),
				module.getBindingTransformers(),
				module.getBindingGenerators(),
				module.getMultibinders());
	}

	private static Trie<Scope, Map<Key<?>, Set<Binding<?>>>> restrict(BiPredicate<Scope[], Key<?>> exportsPredicate,
			Trie<Scope, Map<Key<?>, Set<Binding<?>>>> trie) {
		Map<List<Scope>, Map<Key<?>, Key<?>>> exportsMappings = new HashMap<>();
		BiFunction<Scope[], Key<?>, Key<?>> remapping = (path, key) -> {
			if (exportsPredicate.test(path, key)) {
				return key;
			}
			Map<Key<?>, Key<?>> mapping = exportsMappings.computeIfAbsent(List.of(path), $ -> new HashMap<>());
			Key<?> result = mapping.get(key);
			if (result == null) {
				result = Key.ofType(key.getType(), uniqueQualifier(key.getQualifier()));
				mapping.put(key, result);
			}
			return result;
		};
		return remap(remapping, (path, key) -> key, UNSCOPED, trie, Map.of());
	}

	private static Trie<Scope, Map<Key<?>, Set<Binding<?>>>> remap(BiFunction<Scope[], Key<?>, Key<?>> exportsMapping, BiFunction<Scope[], Key<?>, Key<?>> importsMapping,
			Scope[] path, Trie<Scope, Map<Key<?>, Set<Binding<?>>>> oldBindingsTrie, Map<Key<?>, Scope[]> upperBindings) {
		Map<Key<?>, Set<Binding<?>>> oldBindingsMap = oldBindingsTrie.get();
		Map<Key<?>, Set<Binding<?>>> newBindingsMap = new HashMap<>();
		Map<Key<?>, Scope[]> bindings = new HashMap<>(upperBindings);
		oldBindingsMap.keySet().forEach(key -> bindings.put(key, path));
		for (Map.Entry<Key<?>, Set<Binding<?>>> entry : oldBindingsMap.entrySet()) {
			Key<?> oldExportKey = entry.getKey();
			Key<?> newExportKey = exportsMapping.apply(path, oldExportKey);
			HashSet<Binding<?>> newBindings = new HashSet<>();
			if (newBindingsMap.put(newExportKey, newBindings) != null) {
				throw new IllegalArgumentException("Duplicate remapping: " + oldExportKey + " -> " + newExportKey);
			}
			boolean changed = false;
			for (Binding<?> oldBinding : entry.getValue()) {
				Set<Key<?>> oldDependencies = oldBinding.getDependencies();
				Set<Key<?>> newDependencies = new HashSet<>(oldDependencies.size());
				for (Key<?> oldDependency : oldDependencies) {
					Scope[] importKeyPath = bindings.get(oldDependency);
					Key<?> newImportKey = importKeyPath != null ?
							exportsMapping.apply(importKeyPath, oldDependency) :
							importsMapping.apply(path, oldDependency);
					changed |= !oldDependency.equals(newImportKey);
					newDependencies.add(newImportKey);
				}
				Binding<?> newBinding = changed ?
						new Binding<>(newDependencies) {
							@Override
							public CompiledBinding<Object> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
								//noinspection unchecked
								return (CompiledBinding<Object>) oldBinding.compile(
										new CompiledBindingLocator() {
											@Override
											public <Q> @NotNull CompiledBinding<Q> get(Key<Q> oldImportKey) {
												Scope[] importKeyPath = bindings.get(oldImportKey);
												Key<?> newImportKey = importKeyPath != null ?
														exportsMapping.apply(importKeyPath, oldImportKey) :
														importsMapping.apply(path, oldImportKey);
												//noinspection unchecked
												return compiledBindings.get((Key<Q>) newImportKey);
											}
										},
										threadsafe, scope, slot);
							}
						} :
						oldBinding;
				newBindings.add(newBinding);
			}
		}
		Map<Scope, Trie<Scope, Map<Key<?>, Set<Binding<?>>>>> newChildren = new HashMap<>();
		oldBindingsTrie.getChildren().forEach((key, subtrie) -> newChildren.put(key, remap(exportsMapping, importsMapping,
				next(path, key), subtrie, bindings)));
		return Trie.of(newBindingsMap, newChildren);
	}

}
