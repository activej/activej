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

package io.activej.inject.binding;

import io.activej.common.tuple.*;
import io.activej.inject.Key;
import io.activej.inject.binding.Bindings.*;
import io.activej.inject.impl.*;
import io.activej.inject.util.LocationInfo;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.activej.common.Utils.union;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * A binding is one of the main components of ActiveJ Inject.
 * It boils down to "introspectable function", since it only describes function to create an instance of T from an array of objects and
 * an array of its {@link Key dependencies} in known terms.
 * <p>
 * Also it contains a set of {@link io.activej.inject.module.AbstractModule binding-DSL-like} static factory methods
 * as well as some functional transformations for the ease of creating immutable binding modifications.
 */
@SuppressWarnings({"unused", "WeakerAccess", "Convert2Lambda", "rawtypes"})
public abstract class Binding<T> {
	private final Set<Key<?>> dependencies;
	private BindingType type;

	private @Nullable LocationInfo location;

	protected Binding(Set<Key<?>> dependencies) {
		this(dependencies, BindingType.REGULAR, null);
	}

	protected Binding(Set<Key<?>> dependencies, BindingType type, @Nullable LocationInfo location) {
		this.dependencies = dependencies;
		this.type = type;
		this.location = location;
	}

	public static <T> Binding<T> toInstance(T instance) {
		return new BindingToInstance<>(instance);
	}

	// region Various Binding.to(...) overloads

	public static <T> Binding<T> to(Class<? extends T> key) {
		return Binding.to(Key.of(key));
	}

	public static <T> Binding<T> to(Key<? extends T> key) {
		return new BindingToKey<>(key);
	}

	public static <R> Binding<R> to(TupleConstructorN<R> constructor, Class<?>[] types) {
		return Binding.to(constructor, Stream.of(types).map(Key::of).toArray(Key<?>[]::new));
	}

	public static <R> Binding<R> to(TupleConstructorN<R> constructor, Key<?>[] dependencies) {
		if (dependencies.length == 0) {
			return to(constructor::create);
		}
		return new BindingToConstructorN<>(constructor, dependencies);
	}

	public static <T1, R> Binding<R> to(TupleConstructor1<T1, R> constructor,
			Class<T1> dependency1) {
		return Binding.to(constructor, Key.of(dependency1));
	}

	public static <T1, T2, R> Binding<R> to(TupleConstructor2<T1, T2, R> constructor,
			Class<T1> dependency1, Class<T2> dependency2) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2));
	}

	public static <T1, T2, T3, R> Binding<R> to(TupleConstructor3<T1, T2, T3, R> constructor,
			Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3));
	}

	public static <T1, T2, T3, T4, R> Binding<R> to(TupleConstructor4<T1, T2, T3, T4, R> constructor,
			Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4));
	}

	public static <T1, T2, T3, T4, T5, R> Binding<R> to(TupleConstructor5<T1, T2, T3, T4, T5, R> constructor,
			Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4, Class<T5> dependency5) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4), Key.of(dependency5));
	}

	public static <T1, T2, T3, T4, T5, T6, R> Binding<R> to(TupleConstructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			Class<T1> dependency1, Class<T2> dependency2, Class<T3> dependency3, Class<T4> dependency4, Class<T5> dependency5, Class<T6> dependency6) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4), Key.of(dependency5), Key.of(dependency6));
	}

	public static <R> Binding<R> to(TupleConstructor0<R> constructor) {
		return new BindingToConstructor0<>(constructor);
	}

	public static <T1, R> Binding<R> to(TupleConstructor1<T1, R> constructor,
			Key<T1> dependency1) {
		return new BindingToConstructor1<>(constructor, dependency1);
	}

	public static <T1, T2, R> Binding<R> to(TupleConstructor2<T1, T2, R> constructor,
			Key<T1> dependency1, Key<T2> dependency2) {
		return new BindingToConstructor2<>(dependency1, dependency2, constructor);
	}

	public static <T1, T2, T3, R> Binding<R> to(TupleConstructor3<T1, T2, T3, R> constructor,
			Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3) {
		return new BindingToConstructor3<>(constructor, dependency1, dependency2, dependency3);
	}

	public static <T1, T2, T3, T4, R> Binding<R> to(TupleConstructor4<T1, T2, T3, T4, R> constructor,
			Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4) {
		return new BindingToConstructor4<>(constructor, dependency1, dependency2, dependency3, dependency4);
	}

	public static <T1, T2, T3, T4, T5, R> Binding<R> to(TupleConstructor5<T1, T2, T3, T4, T5, R> constructor,
			Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5) {
		return new BindingToConstructor5<>(constructor, dependency1, dependency2, dependency3, dependency4, dependency5);
	}

	public static <T1, T2, T3, T4, T5, T6, R> Binding<R> to(TupleConstructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5, Key<T6> dependency6) {
		return new BindingToConstructor6<>(constructor, dependency1, dependency2, dependency3, dependency4, dependency5, dependency6);
	}
	// endregion

	public Binding<T> at(@Nullable LocationInfo location) {
		this.location = location;
		return this;
	}

	public Binding<T> as(BindingType type) {
		this.type = type;
		return this;
	}

	public Binding<T> onInstance(Consumer<? super T> consumer) {
		return mapInstance(instance -> {
			consumer.accept(instance);
			return instance;
		});
	}

	public <R> Binding<R> mapInstance(Function<? super T, ? extends R> fn) {
		return new Binding<>(dependencies, type, location) {
			@Override
			public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				CompiledBinding<T> originalBinding = Binding.this.compile(compiledBindings, threadsafe, scope, null);
				return slot != null ?
						new AbstractCompiledBinding<>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								T instance = originalBinding.getInstance(scopedInstances, synchronizedScope);
								return fn.apply(instance);
							}
						} :
						new CompiledBinding<>() {
							@Override
							public R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								T instance = originalBinding.getInstance(scopedInstances, synchronizedScope);
								return fn.apply(instance);
							}
						};
			}
		};
	}

	@SuppressWarnings("Duplicates")
	public <R> Binding<R> mapInstance(List<Key<?>> dependencies, BiFunction<Object[], ? super T, ? extends R> fn) {
		Set<Key<?>> missing = dependencies.stream()
				.filter(required -> !this.dependencies.contains(required))
				.collect(toSet());

		if (!missing.isEmpty()) {
			throw new DIException(missing.stream()
					.map(Key::getDisplayString)
					.collect(joining(", ", "Binding has no dependencies ", " required by mapInstance call")));
		}
		return new Binding<>(this.dependencies, this.type, this.location) {
			@Override
			public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				final CompiledBinding<T> originalBinding = Binding.this.compile(compiledBindings, threadsafe, scope, null);
				final CompiledBinding[] bindings =
						dependencies.stream().map(compiledBindings::get).toArray(CompiledBinding[]::new);

				return slot != null ?
						new AbstractCompiledBinding<>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								Object[] args = new Object[bindings.length];
								for (int i = 0; i < bindings.length; i++) {
									args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
								}
								T instance = originalBinding.getInstance(scopedInstances, synchronizedScope);
								return fn.apply(args, instance);
							}
						} :
						new CompiledBinding<>() {
							@Override
							public R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								Object[] args = new Object[bindings.length];
								for (int i = 0; i < bindings.length; i++) {
									args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
								}
								T instance = originalBinding.getInstance(scopedInstances, synchronizedScope);
								return fn.apply(args, instance);
							}
						};
			}
		};
	}

	public <K> Binding<T> onDependency(Class<K> dependency, Consumer<? super K> consumer) {
		return onDependency(Key.of(dependency), consumer);
	}

	public <K> Binding<T> onDependency(Key<K> dependency, Consumer<? super K> consumer) {
		return mapDependency(dependency, v -> {
			consumer.accept(v);
			return v;
		});
	}

	public <K> Binding<T> mapDependency(Class<K> dependency, Function<? super K, ? extends K> fn) {
		return mapDependency(Key.of(dependency), fn);
	}

	@SuppressWarnings("unchecked")
	public <K> Binding<T> mapDependency(Key<K> dependency, Function<? super K, ? extends K> fn) {
		return new Binding<>(this.dependencies, this.type, this.location) {
			@Override
			public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				return Binding.this.compile(new CompiledBindingLocator() {
					@Override
					public <Q> CompiledBinding<Q> get(Key<Q> key) {
						CompiledBinding<Q> originalBinding = compiledBindings.get(key);
						if (!key.equals(dependency)) return originalBinding;
						return new CompiledBinding<>() {
							@Override
							public Q getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								Q instance = originalBinding.getInstance(scopedInstances, synchronizedScope);
								return (Q) fn.apply((K) instance);
							}
						};
					}
				}, threadsafe, scope, slot);
			}
		};
	}

	public Binding<T> addDependencies(Class<?>... extraDependencies) {
		return addDependencies(Stream.of(extraDependencies).map(Key::of).toArray(Key<?>[]::new));
	}

	public Binding<T> addDependencies(Key<?>... extraDependencies) {
		return addDependencies(Stream.of(extraDependencies).collect(Collectors.toSet()));
	}

	public Binding<T> addDependencies(Set<Key<?>> extraDependencies) {
		return extraDependencies.isEmpty() ?
				this :
				new Binding<>(union(this.dependencies, extraDependencies), this.type, this.location) {
					@Override
					public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
						CompiledBinding<T> compiledBinding = Binding.this.compile(compiledBindings, threadsafe, scope, slot);
						CompiledBinding<?>[] compiledExtraBindings = extraDependencies.stream().map(compiledBindings::get).toArray(CompiledBinding[]::new);
						return new CompiledBinding<>() {
							@Override
							public T getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								//noinspection ForLoopReplaceableByForEach
								for (int i = 0; i < compiledExtraBindings.length; i++) {
									compiledExtraBindings[i].getInstance(scopedInstances, synchronizedScope);
								}
								return compiledBinding.getInstance(scopedInstances, synchronizedScope);
							}
						};
					}
				};
	}

	public Binding<T> initializeWith(BindingInitializer<T> bindingInitializer) {
		return bindingInitializer == BindingInitializer.noop() ?
				this :
				new Binding<>(union(this.dependencies, bindingInitializer.getDependencies()), this.type, this.location) {
					@Override
					public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
						final CompiledBinding<T> compiledBinding = Binding.this.compile(compiledBindings, threadsafe, scope, null);
						final CompiledBindingInitializer<T> consumer = bindingInitializer.compile(compiledBindings);

						return slot != null ?
								new AbstractCompiledBinding<>(scope, slot) {
									@Override
									protected T doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
										T instance = compiledBinding.getInstance(scopedInstances, synchronizedScope);
										consumer.initInstance(instance, scopedInstances, synchronizedScope);
										return instance;
									}
								} :
								new CompiledBinding<>() {
									@Override
									public T getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
										T instance = compiledBinding.getInstance(scopedInstances, synchronizedScope);
										consumer.initInstance(instance, scopedInstances, synchronizedScope);
										return instance;
									}
								};
					}
				};
	}

	public abstract CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot);

	public Set<Key<?>> getDependencies() {
		return dependencies;
	}

	public boolean hasDependency(Key<?> dependency) {
		return dependencies.stream().anyMatch(Predicate.isEqual(dependency));
	}

	public BindingType getType() {
		return type;
	}

	public @Nullable LocationInfo getLocation() {
		return location;
	}

	public String getDisplayString() {
		return dependencies.stream().map(Key::getDisplayString).collect(joining(", ", "[", "]"));
	}

	@Override
	public String toString() {
		return "Binding" + dependencies.toString();
	}

}
