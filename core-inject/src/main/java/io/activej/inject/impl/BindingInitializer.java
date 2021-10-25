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

import io.activej.inject.Key;
import io.activej.inject.binding.Binding;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;

/**
 * This is a {@link Binding} binding modifying function, that can add extra dependencies to it
 * and run initialization code for instance after it was created.
 */
@SuppressWarnings({"rawtypes", "Convert2Lambda"})
public final class BindingInitializer<T> {
	private static final BindingInitializer<?> NOOP = new BindingInitializer<>(emptySet(),
			compiledBindings -> new CompiledBindingInitializer<Object>() {
				@Override
				public void initInstance(Object instance, AtomicReferenceArray[] instances, int synchronizedScope) {
					// noop
				}
			});

	private final Set<Key<?>> dependencies;
	private final BindingInitializerCompiler<T> compiler;

	private BindingInitializer(Set<Key<?>> dependencies, BindingInitializerCompiler<T> compiler) {
		this.dependencies = dependencies;
		this.compiler = compiler;
	}

	public Set<Key<?>> getDependencies() {
		return dependencies;
	}

	public BindingInitializerCompiler<T> getCompiler() {
		return compiler;
	}

	public static <T> BindingInitializer<T> of(Set<Key<?>> dependencies, BindingInitializerCompiler<T> bindingInitializerCompiler) {
		return new BindingInitializer<>(dependencies, bindingInitializerCompiler);
	}

	@SafeVarargs
	public static <T> BindingInitializer<T> combine(BindingInitializer<T>... bindingInitializers) {
		return combine(asList(bindingInitializers));
	}

	@SuppressWarnings("unchecked")
	public static <T> BindingInitializer<T> combine(Collection<BindingInitializer<T>> bindingInitializers) {
		return new BindingInitializer<>(bindingInitializers.stream().map(BindingInitializer::getDependencies).flatMap(Collection::stream).collect(Collectors.toSet()),
				compiledBindings -> {
					CompiledBindingInitializer<T>[] initializers = bindingInitializers.stream()
							.filter(bindingInitializer -> bindingInitializer != NOOP)
							.map(bindingInitializer -> bindingInitializer.compiler.compile(compiledBindings))
							.toArray(CompiledBindingInitializer[]::new);
					if (initializers.length == 0) return new CompiledBindingInitializer<T>() {
						@Override
						public void initInstance(T instance, AtomicReferenceArray[] instances, int synchronizedScope) {
							// noop
						}
					};
					if (initializers.length == 1) return initializers[0];
					if (initializers.length == 2) {
						CompiledBindingInitializer<T> initializer0 = initializers[0];
						CompiledBindingInitializer<T> initializer1 = initializers[1];
						return new CompiledBindingInitializer<T>() {
							@Override
							public void initInstance(T instance, AtomicReferenceArray[] instances, int synchronizedScope) {
								initializer0.initInstance(instance, instances, synchronizedScope);
								initializer1.initInstance(instance, instances, synchronizedScope);
							}
						};
					}
					return new CompiledBindingInitializer<T>() {
						@Override
						public void initInstance(T instance, AtomicReferenceArray[] instances, int synchronizedScope) {
							for (CompiledBindingInitializer<T> initializer : initializers) {
								initializer.initInstance(instance, instances, synchronizedScope);
							}
						}
					};
				});
	}

	@SuppressWarnings("unchecked")
	public static <T> BindingInitializer<T> noop() {
		return (BindingInitializer<T>) NOOP;
	}
}
