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

import io.activej.inject.InstanceInjector;
import io.activej.inject.InstanceProvider;
import io.activej.inject.Key;
import io.activej.inject.Scope;
import io.activej.inject.annotation.Inject;
import io.activej.inject.binding.*;
import io.activej.inject.impl.*;
import io.activej.inject.util.ReflectionUtils;
import io.activej.inject.util.Trie;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static io.activej.inject.util.ReflectionUtils.generateInjectingInitializer;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;

/**
 * This module provides a set of default generators.
 * <p>
 * The first one tries to generate a binding for any missing key by searching for {@link Inject} constructors.
 * <p>
 * The second one generates any Key&lt;SomeType&gt; instance for SomeType.
 * Its purpose is to get reified types from generics in templated providers.
 * <p>
 * The last two generate appropriate instances for {@link InstanceProvider} and {@link InstanceInjector} requests.
 */
@SuppressWarnings({"Convert2Lambda", "rawtypes"})
public final class DefaultModule implements Module {
	private static final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> emptyTrie = Trie.leaf(new HashMap<>());
	private static final Map<Type, Set<BindingGenerator<?>>> generators = new HashMap<>();

	static {
		// generating bindings for classes that have @Inject constructors/factory methods
		generators.put(Object.class, singleton((bindings, scope, key) -> ReflectionUtils.generateImplicitBinding(key)));

		// generating dummy bindings for reified type requests (can be used in templated providers to get a Key<T> instance)
		generators.put(Key.class, singleton((bindings, scope, key) -> Binding.toInstance(key.getTypeParameter(0))));

		// generating bindings for provider requests
		generators.put(InstanceProvider.class, singleton(
				(bindings, $, key) -> {
					Key<Object> instanceKey = key.getTypeParameter(0).qualified(key.getQualifier());
					Binding<Object> instanceBinding = bindings.get(instanceKey);
					if (instanceBinding == null) {
						return null;
					}
					return new Binding<Object>(singleton(Dependency.implicit(instanceKey, true))) {
						@Override
						public CompiledBinding<Object> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
							return slot != null ?
									new AbstractCompiledBinding<Object>(scope, slot) {
										@Override
										protected Object doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
											CompiledBinding<Object> compiledBinding = compiledBindings.get(instanceKey);
											// ^ this only gets already compiled binding, that's not a binding compilation after injector is compiled
											return new InstanceProviderImpl(instanceKey, compiledBinding, scopedInstances, synchronizedScope);
										}
									} :
									new CompiledBinding<Object>() {
										@Override
										public Object getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {

											// transient bindings for instance provider are useless and nobody should make ones
											// however, things like mapInstance create an intermediate transient compiled bindings of their peers
											// usually they call getInstance just once and then cache the result of their computation (eg. the result of mapping function)
											//
											// anyway all of the above means that its ok here to just get the compiled binding and to not care about caching it

											CompiledBinding<Object> compiledBinding = compiledBindings.get(instanceKey);
											return new InstanceProviderImpl(instanceKey, compiledBinding, scopedInstances, synchronizedScope);
										}
									};
						}
					};
				}
		));

		// generating bindings for injector requests
		generators.put(InstanceInjector.class, singleton(
				(bindings, scope, key) -> {
					Key<Object> instanceKey = key.getTypeParameter(0).qualified(key.getQualifier());
					BindingInitializer<Object> bindingInitializer = generateInjectingInitializer(instanceKey);
					return new Binding<Object>(bindingInitializer.getDependencies()) {
						@Override
						public CompiledBinding<Object> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int synchronizedScope, @Nullable Integer slot) {
							return slot != null ?
									new AbstractCompiledBinding<Object>(synchronizedScope, slot) {
										@Override
										protected Object doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
											CompiledBindingInitializer<Object> compiledBindingInitializer = bindingInitializer.getCompiler().compile(compiledBindings);
											return new InstanceInjectorImpl(instanceKey, compiledBindingInitializer, scopedInstances, synchronizedScope);
										}
									} :
									new CompiledBinding<Object>() {
										@Override
										public Object getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
											CompiledBindingInitializer<Object> compiledBindingInitializer = bindingInitializer.getCompiler().compile(compiledBindings);
											// same as with instance providers
											return new InstanceInjectorImpl(instanceKey, compiledBindingInitializer, scopedInstances, synchronizedScope);
										}
									};
						}
					};
				}
		));
	}

	public static void register(Class<?> key, Set<BindingGenerator<?>> bindingGenerators) {
		generators.put(key, bindingGenerators);
	}

	@Override
	public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings() {
		return emptyTrie;
	}

	@Override
	public Map<Type, Set<BindingTransformer<?>>> getBindingTransformers() {
		return emptyMap();
	}

	@Override
	public Map<Type, Set<BindingGenerator<?>>> getBindingGenerators() {
		return generators;
	}

	@Override
	public Map<Key<?>, Multibinder<?>> getMultibinders() {
		return emptyMap();
	}

	public static class InstanceProviderImpl implements InstanceProvider<Object> {
		private final Key<Object> key;
		private final CompiledBinding<Object> compiledBinding;
		private final AtomicReferenceArray[] scopedInstances;
		private final int synchronizedScope;

		public InstanceProviderImpl(Key<Object> key, CompiledBinding<Object> compiledBinding, AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
			this.key = key;
			this.compiledBinding = compiledBinding;
			this.scopedInstances = scopedInstances;
			this.synchronizedScope = synchronizedScope;
		}

		@Override
		public Key<Object> key() {
			return key;
		}

		@Override
		public Object get() {
			return compiledBinding.getInstance(scopedInstances, synchronizedScope);
		}

		@Override
		public String toString() {
			return "InstanceProvider<" + key.getDisplayString() + ">";
		}
	}

	public static class InstanceInjectorImpl implements InstanceInjector<Object> {
		private final Key<Object> key;
		private final CompiledBindingInitializer<Object> compiledBindingInitializer;
		private final AtomicReferenceArray[] scopedInstances;
		private final int synchronizedScope;

		public InstanceInjectorImpl(Key<Object> key, CompiledBindingInitializer<Object> compiledBindingInitializer, AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
			this.key = key;
			this.compiledBindingInitializer = compiledBindingInitializer;
			this.scopedInstances = scopedInstances;
			this.synchronizedScope = synchronizedScope;
		}

		@Override
		public Key<Object> key() {
			return key;
		}

		@Override
		public void injectInto(Object existingInstance) {
			compiledBindingInitializer.initInstance(existingInstance, scopedInstances, synchronizedScope);
		}

		@Override
		public String toString() {
			return "InstanceInjector<" + key.getDisplayString() + ">";
		}
	}
}
