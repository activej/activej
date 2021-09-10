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
import io.activej.inject.binding.*;
import io.activej.inject.util.LocationInfo;
import io.activej.inject.util.Trie;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Stream;

import static io.activej.inject.Scope.UNSCOPED;
import static io.activej.inject.util.ReflectionUtils.scanClassHierarchy;
import static io.activej.inject.util.Utils.*;
import static io.activej.types.Types.parameterizedType;

@SuppressWarnings("UnusedReturnValue")
final class ModuleBuilderImpl<T> implements ModuleBuilder1<T> {
	private final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.leaf(new HashMap<>());
	private final Map<KeyPattern<?>, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>();
	private final Map<KeyPattern<?>, Set<BindingTransformer<?>>> bindingTransformers = new HashMap<>();
	private final Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>();

	private @Nullable BindingDesc current = null;

	private final String name;
	private final @Nullable StackTraceElement location;

	ModuleBuilderImpl() {
		// builder module is (and should be) never instantiated directly,
		// only by Module.create() and AbstractModule actually
		StackTraceElement[] trace = Thread.currentThread().getStackTrace();
		location = trace.length >= 3 ? trace[3] : null;
		name = getClass().getName();
	}

	ModuleBuilderImpl(String name, @Nullable StackTraceElement location) {
		this.name = name;
		this.location = location;
	}

	private void completePreviousStep() {
		if (current != null) {
			addBinding(current);
			current = null;
		}
	}

	private void addBinding(BindingDesc desc) {
		Set<Binding<?>> bindingSet = bindings
				.computeIfAbsent(desc.scope, $ -> new HashMap<>())
				.get()
				.computeIfAbsent(desc.key, $ -> new HashSet<>());

		if (desc.binding != null) {
			bindingSet.add(desc.type == null ? desc.binding : desc.binding.as(desc.type));
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <U> ModuleBuilder0<U> bind(@NotNull Key<U> key) {
		completePreviousStep();
		current = new BindingDesc(key, null);
		return (ModuleBuilder0<U>) this;
	}

	private BindingDesc ensureCurrent() {
		BindingDesc desc = current;
		checkState(desc != null, "Cannot configure binding before bind(...) call");
		return desc;
	}

	@Override
	public ModuleBuilder1<T> in(Scope[] scope) {
		BindingDesc desc = ensureCurrent();
		if (desc.scope.length != 0) {
			throw new IllegalStateException("Already bound to scope " + getScopeDisplayString(desc.scope));
		}
		desc.scope = scope;
		return this;
	}

	@Override
	public ModuleBuilder1<T> in(@NotNull Scope scope, Scope... scopes) {
		Scope[] joined = new Scope[scopes.length + 1];
		joined[0] = scope;
		System.arraycopy(scopes, 0, joined, 1, scopes.length);
		return in(joined);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ModuleBuilder1<T> in(@NotNull Class<? extends Annotation> annotationClass, Class<?>... annotationClasses) {
		return in(Stream.concat(Stream.of(annotationClass), Arrays.stream((Class<? extends Annotation>[]) annotationClasses)).map(Scope::of).toArray(Scope[]::new));
	}

	@Override
	public ModuleBuilder1<T> to(@NotNull Binding<? extends T> binding) {
		BindingDesc desc = ensureCurrent();
		checkState(desc.binding == null, "Already mapped to a binding");
		if (binding.getLocation() == null) {
			binding.at(LocationInfo.from(this));
		}
		desc.binding = binding;
		return this;
	}

	@Override
	public ModuleBuilder1<T> as(BindingType type) {
		BindingDesc current = ensureCurrent();
		checkState(current.type == null, "Binding was already set to eager or transient");
		current.type = type;
		return this;
	}

	@Override
	public ModuleBuilder scan(@NotNull Class<?> moduleClass, @Nullable Object module) {
		return install(scanClassHierarchy(moduleClass, module).values());
	}

	@Override
	public ModuleBuilder install(Collection<Module> modules) {
		completePreviousStep();
		for (Module module : modules) {
			bindings.addAll(module.getBindings(), bindingMultimapMerger());
			combineMultimap(bindingGenerators, module.getBindingGenerators());
			combineMultimap(bindingTransformers, module.getBindingTransformers());
			mergeMultibinders(multibinders, module.getMultibinders());
		}
		return this;
	}

	@Override
	public <S, E extends S> ModuleBuilder bindIntoSet(Key<S> setOf, Binding<E> binding) {
		completePreviousStep();
		Key<Set<S>> set = Key.ofType(parameterizedType(Set.class, setOf.getType()), setOf.getQualifier());
		addBinding(new BindingDesc(set, binding.mapInstance(Collections::singleton)));
		multibinders.put(set, Multibinders.toSet());
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <E> ModuleBuilder generate(KeyPattern<E> pattern, BindingGenerator<E> bindingGenerator) {
		completePreviousStep();
		bindingGenerators.computeIfAbsent(pattern, $ -> new HashSet<>())
				.add((bindings, scope, key) -> {
					Binding<Object> generated = (Binding<Object>) bindingGenerator.generate(bindings, scope, (Key<E>) key);
					if (generated != null && generated.getLocation() == null) {
						generated.at(LocationInfo.from(this));
					}
					return generated;
				});
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <E> ModuleBuilder transform(KeyPattern<E> pattern, BindingTransformer<E> bindingTransformer) {
		completePreviousStep();
		bindingTransformers.computeIfAbsent(pattern, $ -> new HashSet<>())
				.add((bindings, scope, key, binding) -> {
					Binding<Object> transformed = (Binding<Object>) bindingTransformer.transform(bindings, scope, (Key<E>) key, (Binding<E>) binding);
					if (!binding.equals(transformed) && transformed.getLocation() == null) {
						transformed.at(LocationInfo.from(this));
					}
					return transformed;
				});
		return this;
	}

	@Override
	public <E> ModuleBuilder multibind(Key<E> key, Multibinder<E> multibinder) {
		completePreviousStep();
		multibinders.put(key, multibinder);
		return this;
	}

	@Override
	public Module build() {
		completePreviousStep(); // finish the last binding
		return new Module() {
			@Override
			public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings() {
				return bindings;
			}

			@Override
			public Map<KeyPattern<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
				return bindingGenerators;
			}

			@Override
			public Map<KeyPattern<?>, Set<BindingTransformer<?>>> getBindingTransformers() {
				return bindingTransformers;
			}

			@Override
			public Map<Key<?>, Multibinder<?>> getMultibinders() {
				return multibinders;
			}
		};
	}

	@Override
	public String toString() {
		return name + "(at " + (location != null ? location : "<unknown module location>") + ')';
	}

	private static final class BindingDesc {
		private final Key<?> key;
		private Binding<?> binding;
		private Scope[] scope;
		private BindingType type;

		BindingDesc(Key<?> key, Binding<?> binding) {
			this.key = key;
			this.binding = binding;
			this.scope = UNSCOPED;
		}
	}
}
