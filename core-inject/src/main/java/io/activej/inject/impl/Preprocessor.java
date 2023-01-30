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

package io.activej.inject.impl;

import io.activej.inject.*;
import io.activej.inject.binding.*;
import io.activej.inject.module.UniqueQualifierImpl;
import io.activej.inject.util.ReflectionUtils;
import io.activej.inject.util.Trie;
import io.activej.inject.util.Utils;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Stream;

import static io.activej.common.Utils.first;
import static io.activej.common.Utils.union;
import static io.activej.inject.Scope.UNSCOPED;
import static io.activej.inject.binding.BindingType.SYNTHETIC;
import static io.activej.inject.util.ReflectionUtils.generateInjectingInitializer;
import static io.activej.inject.util.Utils.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * This class contains a set of utils for working with binding graph trie.
 */
public final class Preprocessor {
	private Preprocessor() {}

	/**
	 * This method converts a trie of binding multimaps, that is provided from the modules,
	 * into a trie of binding maps on which the {@link Injector} would actually operate.
	 * <p>
	 * While doing so it also recursively tries to generate missing dependency bindings
	 * using given {@link BindingGenerator generator} and apply given {@link BindingTransformer} to all of them.
	 *
	 * @see BindingGenerators#combinedGenerator
	 * @see BindingTransformers#combinedTransformer
	 */
	public static Trie<Scope, Map<Key<?>, Binding<?>>> reduce(
			Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings,
			Multibinder<?> multibinder,
			BindingTransformer<?> transformer,
			BindingGenerator<?> generator
	) {
		Trie<Scope, Map<Key<?>, Binding<?>>> reduced = Trie.leaf(new HashMap<>());
		reduce(UNSCOPED, Map.of(), bindings, reduced, multibinder, transformer, generator);
		return reduced;
	}

	private static void reduce(
			Scope[] scope, Map<Key<?>, Binding<?>> upper,
			Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings, Trie<Scope, Map<Key<?>, Binding<?>>> reduced,
			Multibinder<?> multibinder,
			BindingTransformer<?> transformer,
			BindingGenerator<?> generator) {

		Map<Key<?>, Set<Binding<?>>> localBindings = bindings.get();

		localBindings.forEach((key, bindingSet) -> resolve(upper, localBindings, reduced.get(), scope, key, bindingSet, multibinder, transformer, generator));

		Map<Key<?>, Binding<?>> nextUpper = override(upper, reduced.get());

		bindings.getChildren().forEach((subScope, subLocalBindings) ->
				reduce(next(scope, subScope), nextUpper, subLocalBindings, reduced.computeIfAbsent(subScope, $ -> new HashMap<>()), multibinder, transformer, generator));
	}

	@SuppressWarnings("unchecked")
	private static @Nullable Binding<?> resolve(
			Map<Key<?>, Binding<?>> upper, Map<Key<?>, Set<Binding<?>>> localBindings, Map<Key<?>, Binding<?>> resolvedBindings,
			Scope[] scope, Key<?> key, @Nullable Set<Binding<?>> bindingSet,
			Multibinder<?> multibinder, BindingTransformer<?> transformer, BindingGenerator<?> generator) {

		// shortest path - if it was already resolved, just return it (also serves as a visited set so graph loops don't cause infinite recursion)
		Binding<?> resolvedBinding = resolvedBindings.get(key);
		if (resolvedBinding != null) {
			return resolvedBinding;
		}

		BindingLocator recursiveLocator = new BindingLocator() {
			@Override
			public <T> @Nullable Binding<T> get(Key<T> key) {
				return (Binding<T>) resolve(upper, localBindings, resolvedBindings, scope, key, localBindings.get(key), multibinder, transformer, generator);
			}
		};

		Binding<?> binding;

		// if it was explicitly bound
		Class<?> rawType = key.getRawType();
		if (bindingSet != null) {
			switch (bindingSet.size()) {
				case 0 -> {
					binding = tryResolve(upper, localBindings, resolvedBindings, scope, key, multibinder, transformer, generator, recursiveLocator, rawType);

					// fail fast because this generation was explicitly requested (though plain `bind(...)` call)
					if (binding == null) {
						throw new DIException("Refused to generate an explicitly requested binding for key " + key.getDisplayString());
					}
				}
				case 1 -> binding = bindingSet.iterator().next();
				default ->
					//noinspection rawtypes
						binding = ((Multibinder) multibinder).multibind(key, bindingSet);
			}
		} else { // or if it was never bound
			// first check if it was already resolved in upper scope
			Binding<?> fromUpper = upper.get(key);
			if (fromUpper != null) {
				return fromUpper;
			}

			binding = tryResolve(upper, localBindings, resolvedBindings, scope, key, multibinder, transformer, generator, recursiveLocator, rawType);

			// if it was not resolved then it's simply unsatisfied and later will be checked
			if (binding == null) {
				return null;
			}
		}

		// transform it (once!)
		//noinspection rawtypes
		Binding<?> transformed = ((BindingTransformer) transformer).transform(recursiveLocator, scope, key, binding);

		// and store it as resolved (also mark as visited)
		resolvedBindings.put(key, transformed);

		// and then recursively walk over its dependencies (so this is a recursive dfs after all)
		for (Key<?> d : transformed.getDependencies()) {
			resolve(upper, localBindings, resolvedBindings, scope, d, localBindings.get(d), multibinder, transformer, generator);
		}

		return transformed;
	}

	private static @Nullable Binding<?> tryResolve(Map<Key<?>, Binding<?>> upper, Map<Key<?>, Set<Binding<?>>> localBindings, Map<Key<?>, Binding<?>> resolvedBindings, Scope[] scope, Key<?> key, Multibinder<?> multibinder, BindingTransformer<?> transformer, BindingGenerator<?> generator, BindingLocator recursiveLocator, Class<?> rawType) {
		Binding<?> binding;
		// try to recursively generate a requested binding
		//noinspection unchecked
		binding = ((BindingGenerator<Object>) generator).generate(recursiveLocator, scope, (Key<Object>) key);

		// try to resolve OptionalDependency
		if (binding == null && rawType == OptionalDependency.class) {
			binding = resolveOptionalDependency(upper, localBindings, resolvedBindings, scope, key, multibinder, transformer, generator);
		}

		// try to resolve InstanceProvider
		if (binding == null && rawType == InstanceProvider.class) {
			binding = resolveInstanceProvider(upper, localBindings, resolvedBindings, scope, key, multibinder, transformer, generator);
		}

		// try to resolve InstanceInjector
		if (binding == null && rawType == InstanceInjector.class) {
			binding = resolveInstanceInjector(key);
		}

		// try to resolve Key
		if (binding == null && rawType == Key.class) {
			binding = Binding.toInstance(key.getTypeParameter(0)).as(SYNTHETIC);
		}

		// try to resolve bindings for classes that may have @Inject constructors/factory methods
		if (binding == null) {
			binding = ReflectionUtils.generateImplicitBinding(key);
		}

		// try to automatically resolve child bindings
		if (binding == null) {
			binding = resolveChildBindings(localBindings, key);
		}

		return binding;
	}

	private static @Nullable Binding<?> resolveChildBindings(Map<Key<?>, Set<Binding<?>>> localBindings, Key<?> key) {
		KeyPattern<?> pattern = KeyPattern.ofType(key.getType(), key.getQualifier());

		Map<Key<?>, Set<Binding<?>>> candidates = new HashMap<>();
		for (Entry<Key<?>, Set<Binding<?>>> entry : localBindings.entrySet()) {
			Key<?> localKey = entry.getKey();
			if (!key.equals(localKey) && pattern.match(localKey)) {
				candidates.put(localKey, entry.getValue());
			}
		}

		Key<?> result = tryReduceCandidates(candidates);

		if (result == null) {
			return null;
		}

		return Binding.to(result).as(SYNTHETIC);
	}

	private static @Nullable Key<?> tryReduceCandidates(Map<Key<?>, Set<Binding<?>>> candidates) {
		Set<Key<?>> candidateKeys = candidates.keySet();
		Set<Key<?>> keysCopy = new HashSet<>(candidateKeys);
		for (Entry<Key<?>, Set<Binding<?>>> entry : candidates.entrySet()) {
			Set<Binding<?>> bindingSet = entry.getValue();
			if (bindingSet.size() == 1) {
				Binding<?> binding = first(bindingSet);
				if (binding instanceof BindingToKey<?> bindingToKey && candidateKeys.contains(bindingToKey.getKey())) {
					keysCopy.remove(entry.getKey());
				}
			}
		}

		if (keysCopy.size() == 1) {
			return first(keysCopy);
		}

		return null;
	}

	@SuppressWarnings({"rawtypes", "Convert2Lambda"})
	private static Binding<?> resolveOptionalDependency(Map<Key<?>, Binding<?>> upper, Map<Key<?>, Set<Binding<?>>> localBindings, Map<Key<?>, Binding<?>> resolvedBindings, Scope[] scope, Key<?> key, Multibinder<?> multibinder, BindingTransformer<?> transformer, BindingGenerator<?> generator) {
		Key<?> instanceKey = key.getTypeParameter(0).qualified(key.getQualifier());
		if (instanceKey.getRawType() == OptionalDependency.class) {
			throw new DIException("Nested optional dependencies are not allowed");
		}
		Binding<?> resolved = resolve(upper, localBindings, resolvedBindings, scope, instanceKey, localBindings.get(instanceKey), multibinder, transformer, generator);
		if (resolved == null) return Binding.toInstance(OptionalDependency.empty());
		return new Binding<OptionalDependency<?>>(Set.of(instanceKey), SYNTHETIC, null) {
			@Override
			public CompiledBinding<OptionalDependency<?>> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				return slot != null ?
						new AbstractCompiledBinding<>(scope, slot) {
							@Override
							protected OptionalDependency<?> doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								CompiledBinding<?> compiledBinding = compiledBindings.get(instanceKey);
								return OptionalDependency.of(compiledBinding.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new CompiledBinding<>() {
							@Override
							public OptionalDependency<?> getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								CompiledBinding<?> compiledBinding = compiledBindings.get(instanceKey);
								return OptionalDependency.of(compiledBinding.getInstance(scopedInstances, synchronizedScope));
							}
						};
			}
		};
	}

	@SuppressWarnings({"rawtypes", "Convert2Lambda"})
	private static @Nullable Binding<?> resolveInstanceProvider(Map<Key<?>, Binding<?>> upper, Map<Key<?>, Set<Binding<?>>> localBindings, Map<Key<?>, Binding<?>> resolvedBindings, Scope[] scope, Key<?> key, Multibinder<?> multibinder, BindingTransformer<?> transformer, BindingGenerator<?> generator) {
		Key<Object> instanceKey = key.getTypeParameter(0).qualified(key.getQualifier());
		Binding<?> resolved = resolve(upper, localBindings, resolvedBindings, scope, instanceKey, localBindings.get(instanceKey), multibinder, transformer, generator);
		if (resolved == null) return null;
		return new Binding<InstanceProvider<?>>(Set.of(instanceKey), SYNTHETIC, null) {
			@Override
			public CompiledBinding<InstanceProvider<?>> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				return slot != null ?
						new AbstractCompiledBinding<>(scope, slot) {
							@Override
							protected InstanceProvider<?> doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								CompiledBinding<Object> compiledBinding = compiledBindings.get(instanceKey);
								// ^ this only gets already compiled binding, that's not a binding compilation after injector is compiled
								return new InstanceProviderImpl<>(instanceKey, compiledBinding, scopedInstances, synchronizedScope);
							}
						} :
						new CompiledBinding<>() {
							@Override
							public InstanceProvider<?> getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {

								// transient bindings for instance provider are useless and nobody should make ones
								// however, things like mapInstance create an intermediate transient compiled bindings of their peers
								// usually they call getInstance just once and then cache the result of their computation (e.g. the result of mapping function)
								//
								// anyway all the above means that it's ok here to just get the compiled binding and to not care about caching it

								CompiledBinding<Object> compiledBinding = compiledBindings.get(instanceKey);
								return new InstanceProviderImpl<>(instanceKey, compiledBinding, scopedInstances, synchronizedScope);
							}
						};
			}
		};
	}

	@SuppressWarnings({"rawtypes", "Convert2Lambda"})
	private static Binding<?> resolveInstanceInjector(Key<?> key) {
		Key<Object> instanceKey = key.getTypeParameter(0).qualified(key.getQualifier());
		BindingInitializer<Object> bindingInitializer = generateInjectingInitializer(instanceKey);
		return new Binding<InstanceInjector<?>>(bindingInitializer.getDependencies(), SYNTHETIC, null) {
			@Override
			public CompiledBinding<InstanceInjector<?>> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int synchronizedScope, @Nullable Integer slot) {
				return slot != null ?
						new AbstractCompiledBinding<>(synchronizedScope, slot) {
							@Override
							protected InstanceInjector<?> doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								CompiledBindingInitializer<Object> compiledBindingInitializer = bindingInitializer.compile(compiledBindings);
								return new InstanceInjectorImpl<>(instanceKey, compiledBindingInitializer, scopedInstances, synchronizedScope);
							}
						} :
						new CompiledBinding<>() {
							@Override
							public InstanceInjector<?> getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								CompiledBindingInitializer<Object> compiledBindingInitializer = bindingInitializer.compile(compiledBindings);
								// same as with instance providers
								return new InstanceInjectorImpl<>(instanceKey, compiledBindingInitializer, scopedInstances, synchronizedScope);
							}
						};
			}
		};
	}

	/**
	 * A method that checks binding graph trie completeness, meaning that no binding references a key that is not present
	 * at same or lower level of the trie, and that there is no cycles in each scope (cycles between scopes are not possible).
	 * <p>
	 * It doesn't throw an error if and only if this graph is complete and being complete means that each binding dependencies should
	 * have a respective binding in the local or upper scope, and that there is no cyclic dependencies.
	 */
	public static void check(Set<Key<?>> known, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		checkUnsatisfied(UNSCOPED, known, bindings);
		checkCycles(UNSCOPED, bindings);
	}

	private static void checkUnsatisfied(Scope[] scope, Set<Key<?>> upperKnown, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {

		class DependencyToBinding {
			final Key<?> dependency;
			final Entry<Key<?>, Binding<?>> keybinding;

			public DependencyToBinding(Key<?> dependency, Entry<Key<?>, Binding<?>> keybinding) {
				this.dependency = dependency;
				this.keybinding = keybinding;
			}
		}

		Set<Key<?>> known = union(upperKnown, bindings.get().keySet());

		Map<? extends Key<?>, Set<Entry<Key<?>, Binding<?>>>> unsatisfied = bindings.get().entrySet().stream()
				.flatMap(e -> e.getValue().getDependencies().stream()
						.filter(dependency -> !known.contains(dependency))
						.map(dependency -> new DependencyToBinding(dependency, e)))

				.collect(toMultimap(dtb -> dtb.dependency, dtb -> dtb.keybinding));

		if (!unsatisfied.isEmpty()) {
			throw new DIException(unsatisfied.entrySet().stream()
					.map(entry -> {
						Key<?> missing = entry.getKey();
						String header = "\tkey " + missing.getDisplayString() + " required to make:\n";

						List<String> mkhs = missingKeyHints.stream()
								.map(it -> it.getHintFor(missing, upperKnown, bindings))
								.filter(Objects::nonNull)
								.collect(toList());

						if (!mkhs.isEmpty()) {
							String prefix = "\t" + new String(new char[getKeyDisplayCenter(missing) + 4]).replace('\0', ' ') + "^- ";
							header += prefix + String.join("\n" + prefix, mkhs) + "\n";
						}

						return entry.getValue().stream()
								.map(keybind -> {
									Key<?> key = keybind.getKey();
									String missingDesc = key.getDisplayString() + " " + Utils.getLocation(keybind.getValue());

									List<String> ehs = errorHints.stream()
											.map(it -> it.getHintFor(keybind, missing, upperKnown, bindings))
											.filter(Objects::nonNull)
											.collect(toList());

									if (!ehs.isEmpty()) {
										String prefix = "\n\t\t" + new String(new char[Utils.getKeyDisplayCenter(key) + 2]).replace('\0', ' ') + "^- ";
										missingDesc += prefix + String.join(prefix, ehs);
									}

									return missingDesc;
								})
								.collect(joining("\n\t\t- ", header + "\t\t- ", ""));
					})
					.collect(joining("\n", "Unsatisfied dependencies detected" + (scope.length != 0 ? " in scope " + getScopeDisplayString(scope) : "") + ":\n", "\n")));
		}

		bindings.getChildren().forEach((subScope, subBindings) -> checkUnsatisfied(next(scope, subScope), known, subBindings));
	}

	private static void checkCycles(Scope[] scope, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		// since no cycles are possible between scopes,
		// we just run a simple dfs that ignores unsatisfied
		// dependencies for each scope independently
		List<Key<?>[]> cycles = collectCycles(bindings.get());

		if (!cycles.isEmpty()) {
			throw new DIException(cycles.stream()
					.map(Utils::drawCycle)
					.collect(joining("\n\n", "Cyclic dependencies detected" + (scope.length != 0 ? " in scope " + getScopeDisplayString(scope) : "") + ":\n\n", "\n")));
		}

		bindings.getChildren()
				.forEach((subScope, subBindings) ->
						checkCycles(next(scope, subScope), subBindings));
	}

	/**
	 * This method performs a simple recursive DFS on given binding graph and returns all found cycles.
	 * <p>
	 * Unsatisfied dependencies are ignored.
	 */
	public static List<Key<?>[]> collectCycles(Map<Key<?>, Binding<?>> bindings) {
		Set<Key<?>> visited = new HashSet<>();
		LinkedHashSet<Key<?>> visiting = new LinkedHashSet<>();
		List<Key<?>[]> cycles = new ArrayList<>();
		// the DAG is not necessarily connected, so we go through any possibly disconnected part
		for (Key<?> key : bindings.keySet()) {
			if (!visited.contains(key)) {
				collectCycles(bindings, visited, visiting, cycles, key);
			}
		}
		return cycles;
	}

	private static void collectCycles(Map<Key<?>, Binding<?>> bindings, Set<Key<?>> visited, LinkedHashSet<Key<?>> visiting, List<Key<?>[]> cycles, Key<?> key) {
		Binding<?> binding = bindings.get(key);
		if (binding == null) {
			// just ignore unsatisfied dependencies as if they never existed
			// (they may be unsatisfied and be checked later by unsatisfied dependency check, or they may just reference some upper scope)
			visited.add(key); // add to visited as a tiny optimization
			return;
		}
		// standard dfs with visited (black) and visiting (grey) sets
		if (visiting.add(key)) {
			for (Key<?> dependency : binding.getDependencies()) {
				if (!visited.contains(dependency) && dependency.getRawType() != InstanceProvider.class) {
					collectCycles(bindings, visited, visiting, cycles, dependency);
				}
			}
			visiting.remove(key);
			visited.add(key);
			return;
		}

		// so at this point visiting set looks something like a -> b -> c -> d -> e -> g (-> c),
		// and in the code below we just get d -> e -> g -> c out of it

		Iterator<Key<?>> backtracked = visiting.iterator();
		int skipped = 0;

		// no .hasNext check since the set must contain the key because above .add call returned false
		while (!backtracked.next().equals(key)) {
			skipped++;
		}
		Key<?>[] cycle = new Key<?>[visiting.size() - skipped];
		for (int i = 0; i < cycle.length - 1; i++) {
			// no .hasNext check either because this happens exactly (size - previous .next calls - 1) times
			cycle[i] = backtracked.next();
		}
		// no key was added to the set because it already was there
		// and that one was consumed by the skipping while loop above,
		// so we just add it manually at the end
		cycle[cycle.length - 1] = key;
		cycles.add(cycle);
	}

	@FunctionalInterface
	public interface MissingKeyHint {

		@Nullable String getHintFor(Key<?> missing, Set<Key<?>> upperKnown, Trie<Scope, Map<Key<?>, Binding<?>>> bindings);
	}

	@FunctionalInterface
	public interface ErrorHint {

		@Nullable String getHintFor(Entry<Key<?>, Binding<?>> keybind, Key<?> missing, Set<Key<?>> upperKnown, Trie<Scope, Map<Key<?>, Binding<?>>> bindings);
	}

	private static final List<MissingKeyHint> missingKeyHints = List.of(
			(missing, upperKnown, bindings) -> {
				if (missing.getRawType() != InstanceProvider.class) {
					return null;
				}
				return "it was not generated because there were no *exported* binding for key " + missing.getTypeParameter(0).getDisplayString();
			}
	);

	private static final List<ErrorHint> errorHints = List.of(
			(keybind, missing, upperKnown, bindings) -> {
				Class<?> rawType = keybind.getKey().getRawType();
				if (Modifier.isStatic(rawType.getModifiers()) || !missing.getRawType().equals(rawType.getEnclosingClass())) {
					return null;
				}
				return "this is a non-static inner class with implicit dependency on its enclosing class";
			},
			(keybind, missing, upperKnown, bindings) -> {
				if (keybind.getKey().getRawType() != InstanceInjector.class) {
					return null;
				}

				Object missingQualifier = missing.getQualifier();
				Type missingType = missing.getType();

				Key<?> priv = Stream.concat(bindings.get().keySet().stream(), upperKnown.stream())
						.filter(k -> k.getQualifier() instanceof UniqueQualifierImpl
								&& k.getType().equals(missingType)
								&& Objects.equals(missingQualifier, ((UniqueQualifierImpl) k.getQualifier()).getOriginalQualifier()))
						.findAny()
						.orElse(null);
				if (priv == null) {
					return null;
				}
				return "instance injectors cannot inject private keys (found private key " + priv.getDisplayString() + " " + Utils.getLocation(bindings.get().get(priv)) + ")";
			}
	);

	@SuppressWarnings("rawtypes")
	public static class InstanceProviderImpl<T> implements InstanceProvider<T> {
		private final Key<T> key;
		private final CompiledBinding<T> compiledBinding;
		private final AtomicReferenceArray[] scopedInstances;
		private final int synchronizedScope;

		public InstanceProviderImpl(Key<T> key, CompiledBinding<T> compiledBinding, AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
			this.key = key;
			this.compiledBinding = compiledBinding;
			this.scopedInstances = scopedInstances;
			this.synchronizedScope = synchronizedScope;
		}

		@Override
		public Key<T> key() {
			return key;
		}

		@Override
		public T get() {
			return compiledBinding.getInstance(scopedInstances, synchronizedScope);
		}

		@Override
		public String toString() {
			return "InstanceProvider<" + key.getDisplayString() + ">";
		}
	}

	@SuppressWarnings("rawtypes")
	public static class InstanceInjectorImpl<T> implements InstanceInjector<T> {
		private final Key<T> key;
		private final CompiledBindingInitializer<T> compiledBindingInitializer;
		private final AtomicReferenceArray[] scopedInstances;
		private final int synchronizedScope;

		public InstanceInjectorImpl(Key<T> key, CompiledBindingInitializer<T> compiledBindingInitializer, AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
			this.key = key;
			this.compiledBindingInitializer = compiledBindingInitializer;
			this.scopedInstances = scopedInstances;
			this.synchronizedScope = synchronizedScope;
		}

		@Override
		public Key<T> key() {
			return key;
		}

		@Override
		public void injectInto(T existingInstance) {
			compiledBindingInitializer.initInstance(existingInstance, scopedInstances, synchronizedScope);
		}

		@Override
		public String toString() {
			return "InstanceInjector<" + key.getDisplayString() + ">";
		}
	}
}
