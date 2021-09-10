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
import io.activej.inject.impl.*;
import io.activej.inject.util.Constructors.*;
import io.activej.inject.util.LocationInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.*;
import java.util.stream.Stream;

import static io.activej.inject.util.Utils.union;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * A binding is one of the main components of ActiveJ Inject.
 * It boils down to "introspectable function", since it only describes function to create an instance of T from an array of objects and
 * an array of its {@link Dependency dependencies} in known terms.
 * <p>
 * Also it contains a set of {@link io.activej.inject.module.AbstractModule binding-DSL-like} static factory methods
 * as well as some functional transformations for the ease of creating immutable binding modifications.
 */
@SuppressWarnings({"unused", "WeakerAccess", "ArraysAsListWithZeroOrOneArgument", "Convert2Lambda", "rawtypes"})
public abstract class Binding<T> {
	private final Set<Dependency> dependencies;
	private BindingType type;

	private @Nullable LocationInfo location;

	protected Binding(@NotNull Set<Dependency> dependencies) {
		this(dependencies, BindingType.REGULAR, null);
	}

	protected Binding(@NotNull Set<Dependency> dependencies, BindingType type, @Nullable LocationInfo location) {
		this.dependencies = dependencies;
		this.type = type;
		this.location = location;
	}

	public static <T> Binding<T> toInstance(@NotNull T instance) {
		return new Binding<T>(emptySet()) {
			@Override
			public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				return slot != null ?
						new CompiledBinding<T>() {
							@SuppressWarnings("unchecked")
							@Override
							public T getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								scopedInstances[scope].lazySet(slot, instance);
								return instance;
							}
						} :
						new CompiledBinding<T>() {
							@Override
							public T getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return instance;
							}
						};
			}
		};
	}

	public static <T> Binding<T> toSupplier(@NotNull Key<? extends Supplier<? extends T>> supplierKey) {
		return Binding.to(Supplier::get, supplierKey);
	}

	public static <T> Binding<T> toSupplier(@NotNull Class<? extends Supplier<? extends T>> supplierType) {
		return Binding.to(Supplier::get, supplierType);
	}

	// region Various Binding.to(...) overloads

	public static <T> Binding<T> to(Class<? extends T> key) {
		return Binding.to(Key.of(key));
	}

	public static <T> Binding<T> to(Key<? extends T> key) {
		return new BindingToKey<>(key);
	}

	public static <R> Binding<R> to(@NotNull ConstructorN<R> constructor, Class<?>[] types) {
		return Binding.to(constructor, Stream.of(types).map(Key::of).map(Dependency::toKey).toArray(Dependency[]::new));
	}

	public static <R> Binding<R> to(@NotNull ConstructorN<R> constructor, Key<?>[] keys) {
		return Binding.to(constructor, Stream.of(keys).map(Dependency::toKey).toArray(Dependency[]::new));
	}

	@SuppressWarnings("Duplicates")
	public static <R> Binding<R> to(@NotNull ConstructorN<R> constructor, Dependency[] dependencies) {
		if (dependencies.length == 0) {
			return to(constructor::create);
		}
		return new Binding<R>(new HashSet<>(asList(dependencies))) {
			@Override
			public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				final CompiledBinding<?>[] bindings = Arrays.stream(dependencies)
						.map(dependency -> compiledBindings.get(dependency.getKey()))
						.toArray(CompiledBinding[]::new);

				return slot != null ? threadsafe ? scope == 0 ?
						new AbstractRootCompiledBinding<R>(slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								Object[] args = new Object[bindings.length];
								for (int i = 0; i < bindings.length; i++) {
									args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
								}
								return constructor.create(args);
							}
						} :
						new AbstractCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								Object[] args = new Object[bindings.length];
								for (int i = 0; i < bindings.length; i++) {
									args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
								}
								return constructor.create(args);
							}
						} :
						new AbstractUnsyncCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								Object[] args = new Object[bindings.length];
								for (int i = 0; i < bindings.length; i++) {
									args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
								}
								return constructor.create(args);
							}
						} :
						new CompiledBinding<R>() {
							@Override
							public R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								Object[] args = new Object[bindings.length];
								for (int i = 0; i < bindings.length; i++) {
									args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
								}
								return constructor.create(args);
							}
						};
			}
		};
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
		return new Binding<R>(emptySet()) {
			@Override
			public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				return slot != null ? threadsafe ? scope == 0 ?
						new AbstractRootCompiledBinding<R>(slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create();
							}
						} :
						new AbstractCompiledBinding<R>(scope, slot) {
							@Override
							protected @Nullable R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {return constructor.create();
							}
						} :
						new AbstractUnsyncCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create();
							}
						} :
						new CompiledBinding<R>() {
							@Override
							public R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create();
							}
						};
			}
		};
	}

	public static <T1, R> Binding<R> to(@NotNull Constructor1<T1, R> constructor,
			@NotNull Key<T1> dependency1) {
		return new Binding<R>(new HashSet<>(asList(Dependency.toKey(dependency1)))) {
			@Override
			public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				final CompiledBinding<T1> binding1 = compiledBindings.get(dependency1);

				return slot != null ? threadsafe ? scope == 0 ?
						new AbstractRootCompiledBinding<R>(slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new AbstractCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new AbstractUnsyncCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new CompiledBinding<R>() {
							@Override
							public R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope));
							}
						};
			}
		};
	}

	@SuppressWarnings("Duplicates")
	public static <T1, T2, R> Binding<R> to(@NotNull Constructor2<T1, T2, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2) {
		return new Binding<R>(new HashSet<>(asList(Dependency.toKey(dependency1), Dependency.toKey(dependency2)))) {
			@Override
			public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				final CompiledBinding<T1> binding1 = compiledBindings.get(dependency1);
				final CompiledBinding<T2> binding2 = compiledBindings.get(dependency2);

				return slot != null ? threadsafe ? scope == 0 ?
						new AbstractRootCompiledBinding<R>(slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new AbstractCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new AbstractUnsyncCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new CompiledBinding<R>() {
							@Override
							public R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope));
							}
						};
			}
		};
	}

	@SuppressWarnings("Duplicates")
	public static <T1, T2, T3, R> Binding<R> to(@NotNull Constructor3<T1, T2, T3, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3) {
		return new Binding<R>(new HashSet<>(asList(Dependency.toKey(dependency1), Dependency.toKey(dependency2), Dependency.toKey(dependency3)))) {
			@Override
			public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				final CompiledBinding<T1> binding1 = compiledBindings.get(dependency1);
				final CompiledBinding<T2> binding2 = compiledBindings.get(dependency2);
				final CompiledBinding<T3> binding3 = compiledBindings.get(dependency3);

				return slot != null ? threadsafe ? scope == 0 ?
						new AbstractRootCompiledBinding<R>(slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new AbstractCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new AbstractUnsyncCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new CompiledBinding<R>() {
							@Override
							public R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope));
							}
						};
			}
		};
	}

	@SuppressWarnings("Duplicates")
	public static <T1, T2, T3, T4, R> Binding<R> to(@NotNull Constructor4<T1, T2, T3, T4, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4) {
		return new Binding<R>(new HashSet<>(asList(Dependency.toKey(dependency1), Dependency.toKey(dependency2), Dependency.toKey(dependency3), Dependency.toKey(dependency4)))) {
			@Override
			public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				final CompiledBinding<T1> binding1 = compiledBindings.get(dependency1);
				final CompiledBinding<T2> binding2 = compiledBindings.get(dependency2);
				final CompiledBinding<T3> binding3 = compiledBindings.get(dependency3);
				final CompiledBinding<T4> binding4 = compiledBindings.get(dependency4);

				return slot != null ? threadsafe ? scope == 0 ?
						new AbstractRootCompiledBinding<R>(slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope),
										binding4.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new AbstractCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope),
										binding4.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new AbstractUnsyncCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope),
										binding4.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new CompiledBinding<R>() {
							@Override
							public R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope),
										binding4.getInstance(scopedInstances, synchronizedScope));
							}
						};
			}
		};
	}

	@SuppressWarnings("Duplicates")
	public static <T1, T2, T3, T4, T5, R> Binding<R> to(@NotNull Constructor5<T1, T2, T3, T4, T5, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5) {
		return new Binding<R>(new HashSet<>(asList(Dependency.toKey(dependency1), Dependency.toKey(dependency2), Dependency.toKey(dependency3), Dependency.toKey(dependency4), Dependency.toKey(dependency5)))) {
			@Override
			public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				final CompiledBinding<T1> binding1 = compiledBindings.get(dependency1);
				final CompiledBinding<T2> binding2 = compiledBindings.get(dependency2);
				final CompiledBinding<T3> binding3 = compiledBindings.get(dependency3);
				final CompiledBinding<T4> binding4 = compiledBindings.get(dependency4);
				final CompiledBinding<T5> binding5 = compiledBindings.get(dependency5);

				return slot != null ? threadsafe ? scope == 0 ?
						new AbstractRootCompiledBinding<R>(slot) {

							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope),
										binding4.getInstance(scopedInstances, synchronizedScope),
										binding5.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new AbstractCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope),
										binding4.getInstance(scopedInstances, synchronizedScope),
										binding5.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new AbstractUnsyncCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope),
										binding4.getInstance(scopedInstances, synchronizedScope),
										binding5.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new CompiledBinding<R>() {
							@Override
							public R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope),
										binding4.getInstance(scopedInstances, synchronizedScope),
										binding5.getInstance(scopedInstances, synchronizedScope));
							}
						};
			}
		};
	}

	@SuppressWarnings("Duplicates")
	public static <T1, T2, T3, T4, T5, T6, R> Binding<R> to(@NotNull Constructor6<T1, T2, T3, T4, T5, T6, R> constructor,
			@NotNull Key<T1> dependency1, @NotNull Key<T2> dependency2, @NotNull Key<T3> dependency3, @NotNull Key<T4> dependency4, @NotNull Key<T5> dependency5, @NotNull Key<T6> dependency6) {
		return new Binding<R>(new HashSet<>(asList(Dependency.toKey(dependency1), Dependency.toKey(dependency2), Dependency.toKey(dependency3), Dependency.toKey(dependency4), Dependency.toKey(dependency5), Dependency.toKey(dependency6)))) {
			@Override
			public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				final CompiledBinding<T1> binding1 = compiledBindings.get(dependency1);
				final CompiledBinding<T2> binding2 = compiledBindings.get(dependency2);
				final CompiledBinding<T3> binding3 = compiledBindings.get(dependency3);
				final CompiledBinding<T4> binding4 = compiledBindings.get(dependency4);
				final CompiledBinding<T5> binding5 = compiledBindings.get(dependency5);
				final CompiledBinding<T6> binding6 = compiledBindings.get(dependency6);

				return slot != null ? threadsafe ? scope == 0 ?
						new AbstractRootCompiledBinding<R>(slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope),
										binding4.getInstance(scopedInstances, synchronizedScope),
										binding5.getInstance(scopedInstances, synchronizedScope),
										binding6.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new AbstractCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope),
										binding4.getInstance(scopedInstances, synchronizedScope),
										binding5.getInstance(scopedInstances, synchronizedScope),
										binding6.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new AbstractUnsyncCompiledBinding<R>(scope, slot) {
							@Override
							protected R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope),
										binding4.getInstance(scopedInstances, synchronizedScope),
										binding5.getInstance(scopedInstances, synchronizedScope),
										binding6.getInstance(scopedInstances, synchronizedScope));
							}
						} :
						new CompiledBinding<R>() {
							@Override
							public R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								return constructor.create(
										binding1.getInstance(scopedInstances, synchronizedScope),
										binding2.getInstance(scopedInstances, synchronizedScope),
										binding3.getInstance(scopedInstances, synchronizedScope),
										binding4.getInstance(scopedInstances, synchronizedScope),
										binding5.getInstance(scopedInstances, synchronizedScope),
										binding6.getInstance(scopedInstances, synchronizedScope));
							}
						};
			}
		};
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

	public Binding<T> onInstance(@NotNull Consumer<? super T> consumer) {
		return mapInstance(null, (args, instance) -> {
			consumer.accept(instance);
			return instance;
		});
	}

	public <R> Binding<R> mapInstance(@NotNull Function<? super T, ? extends R> fn) {
		return mapInstance(null, (args, instance) -> fn.apply(instance));
	}

	@SuppressWarnings("Duplicates")
	public <R> Binding<R> mapInstance(@Nullable List<Key<?>> dependencies, @NotNull BiFunction<Object[], ? super T, ? extends R> fn) {
		if (dependencies != null) {
			Set<Key<?>> missing = dependencies.stream()
					.filter(required -> this.dependencies.stream().noneMatch(existing -> existing.getKey().equals(required)))
					.collect(toSet());

			if (!missing.isEmpty()) {
				throw new DIException(missing.stream()
						.map(Key::getDisplayString)
						.collect(joining(", ", "Binding has no dependencies ", " required by mapInstance call")));
			}
		}
		return new Binding<R>(this.dependencies, this.type, this.location) {
			@Override
			public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				final CompiledBinding<T> originalBinding = Binding.this.compile(compiledBindings, threadsafe, scope, null);
				final CompiledBinding[] bindings =
						dependencies != null ?
								dependencies.stream().map(compiledBindings::get).toArray(CompiledBinding[]::new) :
								null;

				return bindings != null ?
						slot != null ?
								new AbstractCompiledBinding<R>(scope, slot) {
									@Override
									protected @Nullable R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
										Object[] args = new Object[bindings.length];
										for (int i = 0; i < bindings.length; i++) {
											args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
										}
										T instance = originalBinding.getInstance(scopedInstances, synchronizedScope);
										return instance != null ? fn.apply(args, instance) : null;
									}
								} :
								new CompiledBinding<R>() {
									@Override
									public @Nullable R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
										Object[] args = new Object[bindings.length];
										for (int i = 0; i < bindings.length; i++) {
											args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
										}
										T instance = originalBinding.getInstance(scopedInstances, synchronizedScope);
										return instance != null ? fn.apply(args, instance) : null;
									}
								} :
						slot != null ?
								new AbstractCompiledBinding<R>(scope, slot) {
									@Override
									protected @Nullable R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
										T instance = originalBinding.getInstance(scopedInstances, synchronizedScope);
										return instance != null ? fn.apply(null, instance) : null;
									}
								} :
								new CompiledBinding<R>() {
									@Override
									public @Nullable R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
										T instance = originalBinding.getInstance(scopedInstances, synchronizedScope);
										return instance != null ? fn.apply(null, instance) : null;
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
		return new Binding<T>(this.dependencies, this.type, this.location) {
			@Override
			public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
				return Binding.this.compile(new CompiledBindingLocator() {
					@Override
					public @NotNull <Q> CompiledBinding<Q> get(Key<Q> key) {
						CompiledBinding<Q> originalBinding = compiledBindings.get(key);
						if (!key.equals(dependency)) return originalBinding;
						return new CompiledBinding<Q>() {
							@Override
							public @Nullable Q getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
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
		return addDependencies(Stream.of(extraDependencies).map(Key::of).map(Dependency::toKey).toArray(Dependency[]::new));
	}

	public Binding<T> addDependencies(Key<?>... extraDependencies) {
		return addDependencies(Stream.of(extraDependencies).map(Dependency::toKey).toArray(Dependency[]::new));
	}

	public Binding<T> addDependencies(Dependency... extraDependencies) {
		return addDependencies(Stream.of(extraDependencies).collect(toSet()));
	}

	public Binding<T> addDependencies(@NotNull Set<Dependency> extraDependencies) {
		return extraDependencies.isEmpty() ?
				this :
				new Binding<T>(union(this.dependencies, extraDependencies), this.type, this.location) {
					@Override
					public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
						CompiledBinding<T> compiledBinding = Binding.this.compile(compiledBindings, threadsafe, scope, slot);
						CompiledBinding<?>[] compiledExtraBindings = extraDependencies.stream().map(d -> compiledBindings.get(d.getKey())).toArray(CompiledBinding[]::new);
						return new CompiledBinding<T>() {
							@Override
							public T getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
								for (CompiledBinding<?> compiledExtraBinding : compiledExtraBindings) {
									compiledExtraBinding.getInstance(scopedInstances, synchronizedScope);
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
				new Binding<T>(union(this.dependencies, bindingInitializer.getDependencies()), this.type, this.location) {
					@Override
					public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
						final CompiledBinding<T> compiledBinding = Binding.this.compile(compiledBindings, threadsafe, scope, null);
						final CompiledBindingInitializer<T> consumer = bindingInitializer.getCompiler().compile(compiledBindings);

						return slot != null ?
								new AbstractCompiledBinding<T>(scope, slot) {
									@Override
									protected T doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
										T instance = compiledBinding.getInstance(scopedInstances, synchronizedScope);
										consumer.initInstance(instance, scopedInstances, synchronizedScope);
										return instance;
									}
								} :
								new CompiledBinding<T>() {
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

	public @NotNull Set<Dependency> getDependencies() {
		return dependencies;
	}

	public @NotNull Set<Key<?>> getDependencyKeys() {
		return dependencies.stream().map(Dependency::getKey).collect(toSet());
	}

	public boolean hasDependency(Key<?> dependency) {
		return dependencies.stream().map(Dependency::getKey).anyMatch(Predicate.isEqual(dependency));
	}

	public BindingType getType() {
		return type;
	}

	public @Nullable LocationInfo getLocation() {
		return location;
	}

	public String getDisplayString() {
		return dependencies.stream().map(Dependency::getDisplayString).collect(joining(", ", "[", "]"));
	}

	@Override
	public String toString() {
		return "Binding" + dependencies.toString();
	}

}
