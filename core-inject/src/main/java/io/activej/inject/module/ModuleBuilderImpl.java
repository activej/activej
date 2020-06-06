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
import io.activej.inject.Scope;
import io.activej.inject.binding.*;
import io.activej.inject.util.LocationInfo;
import io.activej.inject.util.Trie;
import io.activej.inject.util.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Stream;

import static io.activej.inject.binding.BindingType.*;
import static io.activej.inject.impl.CompiledBinding.missingOptionalBinding;
import static io.activej.inject.util.ReflectionUtils.scanClassHierarchy;
import static io.activej.inject.util.Utils.*;
import static java.util.Collections.emptySet;

@SuppressWarnings("UnusedReturnValue")
final class ModuleBuilderImpl<T> implements ModuleBuilder0<T> {
	private static final Binding<?> TO_BE_GENERATED = new Binding<>(emptySet(), (compiledBindings, threadsafe, scope, slot) -> missingOptionalBinding());

	private final Trie<Scope, Map<Key<?>, BindingSet<?>>> bindings = Trie.leaf(new HashMap<>());
	private final Map<Integer, Set<BindingTransformer<?>>> bindingTransformers = new HashMap<>();
	private final Map<Class<?>, Set<BindingGenerator<?>>> bindingGenerators = new HashMap<>();
	private final Map<Key<?>, Multibinder<?>> multibinders = new HashMap<>();

	@Nullable
	private BindingDesc current = null;

	private final String name;
	@Nullable
	private final StackTraceElement location;

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
			addBindingDesc(current);
			current = null;
		}
	}

	private void addBindingDesc(BindingDesc desc) {
		BindingSet<?> bindingSet = bindings
				.computeIfAbsent(desc.getScope(), $ -> new HashMap<>())
				.get()
				.computeIfAbsent(desc.getKey(), $ -> new BindingSet<>(new HashSet<>(), REGULAR));

		bindingSet.setType(desc.getType());

		Binding<?> binding = desc.getBinding();
		if (binding != TO_BE_GENERATED) {
			//noinspection rawtypes,unchecked
			bindingSet.getBindings().add((Binding) binding);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <U> ModuleBuilder0<U> bind(@NotNull Key<U> key) {
		completePreviousStep();
		current = new BindingDesc(key, TO_BE_GENERATED);
		return (ModuleBuilder0<U>) this;
	}

	private BindingDesc ensureCurrent() {
		BindingDesc desc = current;
		checkState(desc != null, "Cannot configure binding before bind(...) call");
		return desc;
	}

	@Override
	public ModuleBuilder0<T> qualified(@NotNull Object qualifier) {
		BindingDesc desc = ensureCurrent();
		Key<?> key = desc.getKey();
		if (key.getQualifier() != null) {
			throw new IllegalStateException("Already qualified with " + getDisplayString(qualifier));
		}
		desc.setKey(key.qualified(qualifier));
		return this;
	}

	@Override
	public ModuleBuilder0<T> in(@NotNull Scope[] scope) {
		BindingDesc desc = ensureCurrent();
		if (desc.getScope().length != 0) {
			throw new IllegalStateException("Already bound to scope " + getScopeDisplayString(desc.getScope()));
		}
		desc.setScope(scope);
		return this;
	}

	@Override
	public ModuleBuilder0<T> in(@NotNull Scope scope, @NotNull Scope... scopes) {
		Scope[] joined = new Scope[scopes.length + 1];
		joined[0] = scope;
		System.arraycopy(scopes, 0, joined, 1, scopes.length);
		return in(joined);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final ModuleBuilder0<T> in(@NotNull Class<? extends Annotation> annotationClass, @NotNull Class<?>... annotationClasses) {
		return in(Stream.concat(Stream.of(annotationClass), Arrays.stream((Class<? extends Annotation>[]) annotationClasses)).map(Scope::of).toArray(Scope[]::new));
	}

	@Override
	public ModuleBuilder0<T> to(@NotNull Binding<? extends T> binding) {
		BindingDesc desc = ensureCurrent();
		checkState(desc.getBinding() == TO_BE_GENERATED, "Already mapped to a binding");
		if (binding.getLocation() == null) {
			binding.at(LocationInfo.from(this));
		}
		desc.setBinding(binding);
		return this;
	}

	@Override
	public ModuleBuilder0<T> asEager() {
		BindingDesc current = ensureCurrent();
		checkState(current.getType() == REGULAR, "Binding was already set to eager or transient");
		current.setType(EAGER);
		return this;
	}

	@Override
	public ModuleBuilder0<T> asTransient() {
		BindingDesc current = ensureCurrent();
		checkState(current.getType() == REGULAR, "Binding was already set to transient or eager");
		current.setType(TRANSIENT);
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
			combineMultimap(bindingTransformers, module.getBindingTransformers());
			combineMultimap(bindingGenerators, module.getBindingGenerators());
			mergeMultibinders(multibinders, module.getMultibinders());
		}
		return this;
	}

	@Override
	public <S, E extends S> ModuleBuilder bindIntoSet(Key<S> setOf, Binding<E> binding) {
		completePreviousStep();
		Key<Set<S>> set = Key.ofType(Types.parameterized(Set.class, setOf.getType()), setOf.getQualifier());
		addBindingDesc(new BindingDesc(set, binding.mapInstance(Collections::singleton)));
		multibinders.put(set, Multibinder.toSet());
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <E> ModuleBuilder transform(int priority, BindingTransformer<E> bindingTransformer) {
		completePreviousStep();
		bindingTransformers.computeIfAbsent(priority, $ -> new HashSet<>())
				.add((bindings, scope, key, binding) -> {
					Binding<Object> transformed = (Binding<Object>) bindingTransformer.transform(bindings, scope, (Key<E>) key, (Binding<E>) binding);
					if (!binding.equals(transformed) && transformed.getLocation() == null) {
						transformed.at(LocationInfo.from(this));
					}
					return transformed;
				});
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <E> ModuleBuilder generate(Class<?> pattern, BindingGenerator<E> bindingGenerator) {
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
			public final Trie<Scope, Map<Key<?>, BindingSet<?>>> getBindings() {
				return bindings;
			}

			@Override
			public final Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers() {
				return bindingTransformers;
			}

			@Override
			public final Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
				return bindingGenerators;
			}

			@Override
			public final Map<Key<?>, Multibinder<?>> getMultibinders() {
				return multibinders;
			}
		};
	}

	@Override
	public String toString() {
		return name + "(at " + (location != null ? location : "<unknown module location>") + ')';
	}
}
