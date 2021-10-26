package io.activej.inject.binding;

import io.activej.inject.Key;
import io.activej.inject.impl.*;
import io.activej.inject.util.Constructors.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReferenceArray;

@SuppressWarnings({"unchecked", "rawtypes", "Convert2Lambda", "ArraysAsListWithZeroOrOneArgument"})
class Bindings {

	public static class BindingToInstance<T> extends Binding<T> {
		final T instance;

		BindingToInstance(T instance) {
			super(Collections.emptySet());
			this.instance = instance;
		}

		@Override
		public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
			final T instance = this.instance;
			return slot != null ?
					new CompiledBinding<T>() {
						@Override
						public @NotNull T getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							scopedInstances[scope].lazySet(slot, instance);
							return instance;
						}
					} :
					new CompiledBinding<T>() {
						@Override
						public @NotNull T getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return instance;
						}
					};
		}
	}

	public static class BindingToConstructorN<R> extends Binding<R> {
		final ConstructorN<R> constructor;
		final Key<?>[] dependencies;

		BindingToConstructorN(ConstructorN<R> constructor, Key<?>[] dependencies) {
			super(new HashSet<>(Arrays.asList(dependencies)));
			this.constructor = constructor;
			this.dependencies = dependencies;
		}

		@Override
		public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
			final ConstructorN<R> constructor = this.constructor;
			final CompiledBinding<?>[] bindings = Arrays.stream(dependencies)
					.map(compiledBindings::get)
					.toArray(CompiledBinding[]::new);
			return slot != null ? threadsafe ? scope == 0 ?
					new AbstractRootCompiledBinding<R>(slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							Object[] args = new Object[bindings.length];
							for (int i = 0; i < bindings.length; i++) {
								args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
							}
							return constructor.create(args);
						}
					} :
					new AbstractCompiledBinding<R>(scope, slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							Object[] args = new Object[bindings.length];
							for (int i = 0; i < bindings.length; i++) {
								args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
							}
							return constructor.create(args);
						}
					} :
					new AbstractUnsyncCompiledBinding<R>(scope, slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							Object[] args = new Object[bindings.length];
							for (int i = 0; i < bindings.length; i++) {
								args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
							}
							return constructor.create(args);
						}
					} :
					new CompiledBinding<R>() {
						@Override
						public @NotNull R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							Object[] args = new Object[bindings.length];
							for (int i = 0; i < bindings.length; i++) {
								args[i] = bindings[i].getInstance(scopedInstances, synchronizedScope);
							}
							return constructor.create(args);
						}
					};
		}
	}

	public static class BindingToConstructor0<R> extends Binding<R> {
		final Constructor0<R> constructor;

		BindingToConstructor0(Constructor0<R> constructor) {
			super(Collections.emptySet());
			this.constructor = constructor;
		}

		@Override
		public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
			final Constructor0<R> constructor = this.constructor;
			return slot != null ? threadsafe ? scope == 0 ?
					new AbstractRootCompiledBinding<R>(slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create();
						}
					} :
					new AbstractCompiledBinding<R>(scope, slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create();
						}
					} :
					new AbstractUnsyncCompiledBinding<R>(scope, slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create();
						}
					} :
					new CompiledBinding<R>() {
						@Override
						public @NotNull R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create();
						}
					};
		}
	}

	public static class BindingToConstructor1<R, T1> extends Binding<R> {
		final Constructor1<T1, R> constructor;
		final Key<T1> dependency1;

		BindingToConstructor1(Constructor1<T1, R> constructor, Key<T1> dependency1) {
			super(new HashSet<>(Arrays.asList(dependency1)));
			this.constructor = constructor;
			this.dependency1 = dependency1;
		}

		@Override
		public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
			final Constructor1<T1, R> constructor = this.constructor;
			final CompiledBinding<T1> binding1 = compiledBindings.get(dependency1);
			return slot != null ? threadsafe ? scope == 0 ?
					new AbstractRootCompiledBinding<R>(slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new AbstractCompiledBinding<R>(scope, slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new AbstractUnsyncCompiledBinding<R>(scope, slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new CompiledBinding<R>() {
						@Override
						public @NotNull R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope));
						}
					};
		}
	}

	public static class BindingToConstructor2<R, T1, T2> extends Binding<R> {
		final Constructor2<T1, T2, R> constructor;
		final Key<T1> dependency1;
		final Key<T2> dependency2;

		BindingToConstructor2(Key<T1> dependency1, Key<T2> dependency2, Constructor2<T1, T2, R> constructor) {
			super(new HashSet<>(Arrays.asList(dependency1, dependency2)));
			this.constructor = constructor;
			this.dependency1 = dependency1;
			this.dependency2 = dependency2;
		}

		@Override
		public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
			final Constructor2<T1, T2, R> constructor = this.constructor;
			final CompiledBinding<T1> binding1 = compiledBindings.get(dependency1);
			final CompiledBinding<T2> binding2 = compiledBindings.get(dependency2);
			return slot != null ? threadsafe ? scope == 0 ?
					new AbstractRootCompiledBinding<R>(slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new AbstractCompiledBinding<R>(scope, slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new AbstractUnsyncCompiledBinding<R>(scope, slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new CompiledBinding<R>() {
						@Override
						public @NotNull R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope));
						}
					};
		}
	}

	public static class BindingToConstructor3<R, T1, T2, T3> extends Binding<R> {
		final Constructor3<T1, T2, T3, R> constructor;
		final Key<T1> dependency1;
		final Key<T2> dependency2;
		final Key<T3> dependency3;

		BindingToConstructor3(Constructor3<T1, T2, T3, R> constructor, Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3) {
			super(new HashSet<>(Arrays.asList(dependency1, dependency2, dependency3)));
			this.dependency1 = dependency1;
			this.dependency2 = dependency2;
			this.dependency3 = dependency3;
			this.constructor = constructor;
		}

		@Override
		public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
			final Constructor3<T1, T2, T3, R> constructor = this.constructor;
			final CompiledBinding<T1> binding1 = compiledBindings.get(dependency1);
			final CompiledBinding<T2> binding2 = compiledBindings.get(dependency2);
			final CompiledBinding<T3> binding3 = compiledBindings.get(dependency3);
			return slot != null ? threadsafe ? scope == 0 ?
					new AbstractRootCompiledBinding<R>(slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope),
									binding3.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new AbstractCompiledBinding<R>(scope, slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope),
									binding3.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new AbstractUnsyncCompiledBinding<R>(scope, slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope),
									binding3.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new CompiledBinding<R>() {
						@Override
						public @NotNull R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope),
									binding3.getInstance(scopedInstances, synchronizedScope));
						}
					};
		}
	}

	public static class BindingToConstructor4<R, T1, T2, T3, T4> extends Binding<R> {
		final Constructor4<T1, T2, T3, T4, R> constructor;
		final Key<T1> dependency1;
		final Key<T2> dependency2;
		final Key<T3> dependency3;
		final Key<T4> dependency4;

		BindingToConstructor4(Constructor4<T1, T2, T3, T4, R> constructor, Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4) {
			super(new HashSet<>(Arrays.asList(dependency1, dependency2, dependency3, dependency4)));
			this.constructor = constructor;
			this.dependency1 = dependency1;
			this.dependency2 = dependency2;
			this.dependency3 = dependency3;
			this.dependency4 = dependency4;
		}

		@Override
		public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
			final Constructor4<T1, T2, T3, T4, R> constructor = this.constructor;
			final CompiledBinding<T1> binding1 = compiledBindings.get(dependency1);
			final CompiledBinding<T2> binding2 = compiledBindings.get(dependency2);
			final CompiledBinding<T3> binding3 = compiledBindings.get(dependency3);
			final CompiledBinding<T4> binding4 = compiledBindings.get(dependency4);
			return slot != null ? threadsafe ? scope == 0 ?
					new AbstractRootCompiledBinding<R>(slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope),
									binding3.getInstance(scopedInstances, synchronizedScope),
									binding4.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new AbstractCompiledBinding<R>(scope, slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope),
									binding3.getInstance(scopedInstances, synchronizedScope),
									binding4.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new AbstractUnsyncCompiledBinding<R>(scope, slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope),
									binding3.getInstance(scopedInstances, synchronizedScope),
									binding4.getInstance(scopedInstances, synchronizedScope));
						}
					} :
					new CompiledBinding<R>() {
						@Override
						public @NotNull R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope),
									binding3.getInstance(scopedInstances, synchronizedScope),
									binding4.getInstance(scopedInstances, synchronizedScope));
						}
					};
		}
	}

	public static class BindingToConstructor5<R, T1, T2, T3, T4, T5> extends Binding<R> {
		final Constructor5<T1, T2, T3, T4, T5, R> constructor;
		final Key<T1> dependency1;
		final Key<T2> dependency2;
		final Key<T3> dependency3;
		final Key<T4> dependency4;
		final Key<T5> dependency5;

		BindingToConstructor5(Constructor5<T1, T2, T3, T4, T5, R> constructor, Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5) {
			super(new HashSet<>(Arrays.asList(dependency1, dependency2, dependency3, dependency4, dependency5)));
			this.constructor = constructor;
			this.dependency1 = dependency1;
			this.dependency2 = dependency2;
			this.dependency3 = dependency3;
			this.dependency4 = dependency4;
			this.dependency5 = dependency5;
		}

		@Override
		public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
			final Constructor5<T1, T2, T3, T4, T5, R> constructor = this.constructor;
			final CompiledBinding<T1> binding1 = compiledBindings.get(dependency1);
			final CompiledBinding<T2> binding2 = compiledBindings.get(dependency2);
			final CompiledBinding<T3> binding3 = compiledBindings.get(dependency3);
			final CompiledBinding<T4> binding4 = compiledBindings.get(dependency4);
			final CompiledBinding<T5> binding5 = compiledBindings.get(dependency5);
			return slot != null ? threadsafe ? scope == 0 ?
					new AbstractRootCompiledBinding<R>(slot) {

						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
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
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
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
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
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
						public @NotNull R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
							return constructor.create(
									binding1.getInstance(scopedInstances, synchronizedScope),
									binding2.getInstance(scopedInstances, synchronizedScope),
									binding3.getInstance(scopedInstances, synchronizedScope),
									binding4.getInstance(scopedInstances, synchronizedScope),
									binding5.getInstance(scopedInstances, synchronizedScope));
						}
					};
		}
	}

	public static class BindingToConstructor6<R, T1, T2, T3, T4, T5, T6> extends Binding<R> {
		final Constructor6<T1, T2, T3, T4, T5, T6, R> constructor;
		final Key<T1> dependency1;
		final Key<T2> dependency2;
		final Key<T3> dependency3;
		final Key<T4> dependency4;
		final Key<T5> dependency5;
		final Key<T6> dependency6;

		BindingToConstructor6(Constructor6<T1, T2, T3, T4, T5, T6, R> constructor, Key<T1> dependency1, Key<T2> dependency2, Key<T3> dependency3, Key<T4> dependency4, Key<T5> dependency5, Key<T6> dependency6) {
			super(new HashSet<>(Arrays.asList(dependency1, dependency2, dependency3, dependency4, dependency5, dependency6)));
			this.constructor = constructor;
			this.dependency1 = dependency1;
			this.dependency2 = dependency2;
			this.dependency3 = dependency3;
			this.dependency4 = dependency4;
			this.dependency5 = dependency5;
			this.dependency6 = dependency6;
		}

		@Override
		public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
			final Constructor6<T1, T2, T3, T4, T5, T6, R> constructor = this.constructor;
			final CompiledBinding<T1> binding1 = compiledBindings.get(dependency1);
			final CompiledBinding<T2> binding2 = compiledBindings.get(dependency2);
			final CompiledBinding<T3> binding3 = compiledBindings.get(dependency3);
			final CompiledBinding<T4> binding4 = compiledBindings.get(dependency4);
			final CompiledBinding<T5> binding5 = compiledBindings.get(dependency5);
			final CompiledBinding<T6> binding6 = compiledBindings.get(dependency6);
			return slot != null ? threadsafe ? scope == 0 ?
					new AbstractRootCompiledBinding<R>(slot) {
						@Override
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
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
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
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
						protected @NotNull R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
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
						public @NotNull R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
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
	}
}
