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

import io.activej.inject.Key;
import io.activej.inject.binding.Bindings.*;
import io.activej.inject.impl.*;
import io.activej.inject.util.Constructors.*;
import io.activej.inject.util.LocationInfo;
import org.jetbrains.annotations.NotNull;
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

import static io.activej.inject.util.Utils.union;
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

	protected Binding(@NotNull Set<Key<?>> dependencies) {
		this(dependencies, BindingType.REGULAR, null);
	}

	protected Binding(@NotNull Set<Key<?>> dependencies, BindingType type, @Nullable LocationInfo location) {
		this.dependencies = dependencies;
		this.type = type;
		this.location = location;
	}

	public static <T> Binding<T> toInstance(@NotNull T instance) {
		return new BindingToInstance<>(instance);
	}

	// region Various Binding.to(...) overloads

	public static <T> Binding<T> to(Class<? extends T> key) {
		return Binding.to(Key.of(key));
	}

	public static <T> Binding<T> to(Key<? extends T> key) {
		return new BindingToKey<>(key);
	}

	public static <R> Binding<R> to(@NotNull ConstructorN<R> constructor, Class<?>[] types) {
		return Binding.to(constructor, Stream.of(types).map(Key::of).toArray(Key<?>[]::new));
	}

	@SuppressWarnings("Duplicates")
	public static <R> Binding<R> to(@NotNull ConstructorN<R> constructor, Key<?>[] dependencies) {
		if (dependencies.length == 0) {
			return to(constructor::create);
		}
		return new BindingToConstructorN<>(constructor, dependencies);
	}

	public static <T1, R> Binding<R> to(@NotNull Constructor1<T1, R> constructor,
			@NotNull Class<T1> dependency1) {
		return Binding.to(constructor, Key.of(dependency1));
	}

	public static <T1, T2, R> Binding<R> to(@NotNull Constructor2<T1, T2, R> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2));
	}

	public static <T1, T2, T3, R> Binding<R> to(@NotNull Constructor3<T1, T2, T3, R> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3));
	}

	public static <T1, T2, T3, T4, R> Binding<R> to(@NotNull Constructor4<T1, T2, T3, T4, R> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4));
	}

	public static <T1, T2, T3, T4, T5, R> Binding<R> to(@NotNull Constructor5<T1, T2, T3, T4, T5, R> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4, @NotNull Class<T5> dependency5) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4), Key.of(dependency5));
	}

	public static <T1, T2, T3, T4, T5, T6, R> Binding<R> to(@NotNull Constructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			@NotNull Class<T1> dependency1, @NotNull Class<T2> dependency2, @NotNull Class<T3> dependency3, @NotNull Class<T4> dependency4, @NotNull Class<T5> dependency5, @NotNull Class<T6> dependency6) {
		return Binding.to(constructor, Key.of(dependency1), Key.of(dependency2), Key.of(dependency3), Key.of(dependency4), Key.of(dependency5), Key.of(dependency6));
	}

	public static <R> Binding<R> to(@NotNull Constructor0<R> constructor) {
		return new BindingToConstructor0<>(constructor);
	}

	public static <T1, R> Binding<R> to(@NotNull Constructor1<T1, R> constructor,
			@NotNull Key<T1> dependency1) {
		return new BindingToConstructor1<>(constructor, dependency1);
	}

	@SuppressWarnings("Duplicates")
	public static <T1, T2, R> Binding<R> to(@NotNull Constructor2<T1, T2, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2) {
		return new BindingToConstructor2<>(dependency1, dependency2, constructor);
	}

	@SuppressWarnings("Duplicates")
	public static <T1, T2, T3, R> Binding<R> to(@NotNull Constructor3<T1, T2, T3, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3) {
		return new BindingToConstructor3<>(constructor, dependency1, dependency2, dependency3);
	}

	@SuppressWarnings("Duplicates")
	public static <T1, T2, T3, T4, R> Binding<R> to(@NotNull Constructor4<T1, T2, T3, T4, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4) {
		return new BindingToConstructor4<>(constructor, dependency1, dependency2, dependency3, dependency4);
	}

	@SuppressWarnings("Duplicates")
	public static <T1, T2, T3, T4, T5, R> Binding<R> to(@NotNull Constructor5<T1, T2, T3, T4, T5, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5) {
		return new BindingToConstructor5<>(constructor, dependency1, dependency2, dependency3, dependency4, dependency5);
	}

	@SuppressWarnings("Duplicates")
	public static <T1, T2, T3, T4, T5, T6, R> Binding<R> to(@NotNull Constructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5, @NotNull Key<T6> dependency6) {
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

	public Binding<T> onInstance(@NotNull Consumer<? super @NotNull T> consumer) {
		return mapInstance(instance -> {
			consumer.accept(instance);
			return instance;
		});
	}

	public <R> Binding<R> mapInstance(@NotNull Function<? super @NotNull T, ? extends @NotNull R> fn) {
		return new Binding<>(dependencies, type, location) {
			@Override
			public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				CompiledBinding<T> originalBinding = Binding.this.compile(compiledBindings, threadsafe, scope, null);
				return slot != null ?
						new AbstractCompiledBinding<R>(scope, slot) {
							@Override
							protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								T instance = originalBinding.getInstance(scopedInstances, synchronizedScope);
								return fn.apply(instance);
							}
						} :
						new CompiledBinding<R>() {
							@Override
							public @NotNull R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								T instance = originalBinding.getInstance(scopedInstances, synchronizedScope);
								return fn.apply(instance);
							}
						};
			}
		};
	}

	@SuppressWarnings("Duplicates")
	public <R> Binding<R> mapInstance(@NotNull List<Key<?>> dependencies, @NotNull BiFunction<Object[], ? super @NotNull T, ? extends @NotNull R> fn) {
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
						new AbstractCompiledBinding<R>(scope, slot) {
							@Override
							protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								Object[] args = new Object[bindings.length];
								for (int i = 0; i < bindings.length; i++) {
									args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
								}
								T instance = originalBinding.getInstance(scopedInstances, synchronizedScope);
								return fn.apply(args, instance);
							}
						} :
						new CompiledBinding<R>() {
							@Override
							public @NotNull R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
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

	public <K> Binding<T> onDependency(@NotNull Class<K> dependency, @NotNull Consumer<? super K> consumer) {
		return onDependency(Key.of(dependency), consumer);
	}

	public <K> Binding<T> onDependency(@NotNull Key<K> dependency, @NotNull Consumer<? super K> consumer) {
		return mapDependency(dependency, v -> {
			consumer.accept(v);
			return v;
		});
	}

	public <K> Binding<T> mapDependency(@NotNull Class<K> dependency, @NotNull Function<? super K, ? extends K> fn) {
		return mapDependency(Key.of(dependency), fn);
	}

	@SuppressWarnings("unchecked")
	public <K> Binding<T> mapDependency(@NotNull Key<K> dependency, @NotNull Function<? super K, ? extends K> fn) {
		return new Binding<>(this.dependencies, this.type, this.location) {
			@Override
			public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				return Binding.this.compile(new CompiledBindingLocator() {
					@Override
					public <Q> @NotNull CompiledBinding<Q> get(Key<Q> key) {
						CompiledBinding<Q> originalBinding = compiledBindings.get(key);
						if (!key.equals(dependency)) return originalBinding;
						return new CompiledBinding<>() {
							@Override
							public @NotNull Q getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
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

	public Binding<T> addDependencies(@NotNull Set<Key<?>> extraDependencies) {
		return extraDependencies.isEmpty() ?
				this :
				new Binding<>(union(this.dependencies, extraDependencies), this.type, this.location) {
					@Override
					public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
						CompiledBinding<T> compiledBinding = Binding.this.compile(compiledBindings, threadsafe, scope, slot);
						CompiledBinding<?>[] compiledExtraBindings = extraDependencies.stream().map(compiledBindings::get).toArray(CompiledBinding[]::new);
						return new CompiledBinding<>() {
							@Override
							public @NotNull T getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
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
								new AbstractCompiledBinding<T>(scope, slot) {
									@Override
									protected @NotNull T doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
										T instance = compiledBinding.getInstance(scopedInstances, synchronizedScope);
										consumer.initInstance(instance, scopedInstances, synchronizedScope);
										return instance;
									}
								} :
								new CompiledBinding<T>() {
									@Override
									public @NotNull T getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
										T instance = compiledBinding.getInstance(scopedInstances, synchronizedScope);
										consumer.initInstance(instance, scopedInstances, synchronizedScope);
										return instance;
									}
								};
					}
				};
	}

	public abstract CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot);

	public @NotNull Set<Key<?>> getDependencies() {
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
