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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

/**
 * This is a {@link Binding} binding modifying function, that can add extra dependencies to it
 * and run initialization code for instance after it was created.
 */
@SuppressWarnings({"rawtypes", "Convert2Lambda"})
public abstract class BindingInitializer<T> {
	private static final BindingInitializer<?> NOOP = new BindingInitializer<Object>(emptySet()) {
		@Override
		public CompiledBindingInitializer<Object> compile(CompiledBindingLocator compiledBindings) {
			return new CompiledBindingInitializer<Object>() {
				@Override
				public void initInstance(Object instance, AtomicReferenceArray[] instances, int synchronizedScope) {
					// noop
				}
			};
		}
	};

	private final Set<Key<?>> dependencies;

	protected BindingInitializer(Set<Key<?>> dependencies) {
		this.dependencies = dependencies;
	}

	public Set<Key<?>> getDependencies() {
		return dependencies;
	}

	public abstract CompiledBindingInitializer<T> compile(CompiledBindingLocator compiledBindings);

	@SafeVarargs
	public static <T> BindingInitializer<T> combine(BindingInitializer<T>... bindingInitializers) {
		return combine(asList(bindingInitializers));
	}

	@SuppressWarnings("unchecked")
	public static <T> BindingInitializer<T> combine(List<BindingInitializer<T>> bindingInitializers) {
		return new BindingInitializer<T>(bindingInitializers.stream().map(BindingInitializer::getDependencies).flatMap(Collection::stream).collect(toSet())) {
			@Override
			public CompiledBindingInitializer<T> compile(CompiledBindingLocator compiledBindings) {
				CompiledBindingInitializer<T>[] initializers = bindingInitializers.stream()
						.filter(bindingInitializer -> bindingInitializer != NOOP)
						.map(bindingInitializer -> bindingInitializer.compile(compiledBindings))
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
						//noinspection ForLoopReplaceableByForEach
						for (int i = 0; i < initializers.length; i++) {
							initializers[i].initInstance(instance, instances, synchronizedScope);
						}
					}
				};
			}
		};
	}

	@SuppressWarnings("unchecked")
	public static <T> BindingInitializer<T> noop() {
		return (BindingInitializer<T>) NOOP;
	}
}
