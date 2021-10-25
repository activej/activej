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
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.binding.*;
import io.activej.inject.util.ReflectionUtils;
import io.activej.inject.util.Trie;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import static io.activej.inject.util.Utils.checkState;

/**
 * This class is an abstract module wrapper around {@link ModuleBuilder}.
 * It provides functionality that is similar to some other DI frameworks for the ease of transition.
 */
@SuppressWarnings({"SameParameterValue", "UnusedReturnValue", "unused"})
public abstract class AbstractModule implements Module {
	private Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings;
	private Map<KeyPattern<?>, Set<BindingGenerator<?>>> bindingGenerators;
	private Map<KeyPattern<?>, Set<BindingTransformer<?>>> bindingTransformers;
	private Map<Key<?>, Multibinder<?>> multibinders;

	private @Nullable ModuleBuilder builder;

	private final @Nullable StackTraceElement location;

	protected AbstractModule() {
		StackTraceElement[] trace = Thread.currentThread().getStackTrace();
		StackTraceElement found = null;
		Class<?> cls = getClass();
		for (int i = 2; i < trace.length; i++) {
			StackTraceElement element = trace[i];
			try {
				String className = element.getClassName();
				Class<?> traceCls = Class.forName(className);
				if (!traceCls.isAssignableFrom(cls) && !className.startsWith("sun.reflect") && !className.startsWith("java.lang")) {
					found = element;
					break;
				}
			} catch (ClassNotFoundException ignored) {
				break;
			}
		}
		this.location = found;
		this.builder = new ModuleBuilderImpl<>(getName(), location);
	}

	/**
	 * This method is meant to be overridden to call all the <code>bind(...)</code> methods.
	 */
	protected void configure() {
	}

	/**
	 * This method begins a chain of binding builder DSL calls
	 *
	 * @see ModuleBuilder#bind(Key)
	 */
	protected final <T> ModuleBuilder0<T> bind(@NotNull Key<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bind(key);
	}

	/**
	 * This method begins a chain of binding builder DSL calls.
	 *
	 * @see ModuleBuilder#bind(Key)
	 */
	protected final <T> ModuleBuilder0<T> bind(Class<T> type) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bind(type);
	}

	/**
	 * This method begins a chain of binding builder DSL calls.
	 *
	 * @see ModuleBuilder#bind(Key)
	 */
	protected final <T> ModuleBuilder0<T> bind(Class<T> type, Object qualifier) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		return builder.bind(type, qualifier);
	}

	protected final <T> void bindInstanceProvider(@NotNull Class<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.bindInstanceProvider(key);
	}

	protected final <T> void bindInstanceProvider(@NotNull Key<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.bindInstanceProvider(key);
	}

	protected final <T> void bindInstanceInjector(@NotNull Class<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.bindInstanceInjector(key);
	}

	protected final <T> void bindInstanceInjector(@NotNull Key<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.bindInstanceInjector(key);
	}

	protected final <T> void bindOptionalDependency(@NotNull Class<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.bindOptionalDependency(key);
	}

	protected final <T> void bindOptionalDependency(@NotNull Key<T> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.bindOptionalDependency(key);
	}

	/**
	 * Adds all bindings, transformers, generators and multibinders from given modules to this one.
	 * <p>
	 * This works just as if you'd define all of those directly in this module.
	 *
	 * @see ModuleBuilder#install
	 */
	protected final void install(Module module) {
		checkState(builder != null, "Cannot install modules before or after configure() call");
		builder.install(module);
	}

	/**
	 * Adds a {@link BindingGenerator generator} for a given class to this module.
	 *
	 * @see ModuleBuilder#generate
	 */
	protected final <T> void generate(KeyPattern<T> pattern, BindingGenerator<T> bindingGenerator) {
		checkState(builder != null, "Cannot add generators before or after configure() call");
		builder.generate(pattern, bindingGenerator);
	}

	protected final <T> void generate(Class<T> pattern, BindingGenerator<T> bindingGenerator) {
		generate(KeyPattern.of(pattern), bindingGenerator);
	}

	/**
	 * Adds a {@link BindingTransformer transformer} with a given type to this module.
	 *
	 * @see ModuleBuilder#transform
	 */
	protected final <T> void transform(KeyPattern<T> pattern, BindingTransformer<T> bindingTransformer) {
		checkState(builder != null, "Cannot add transformers before or after configure() call");
		builder.transform(pattern, bindingTransformer);
	}

	protected final <T> void transform(Class<T> pattern, BindingTransformer<T> bindingTransformer) {
		transform(KeyPattern.of(pattern), bindingTransformer);
	}

	/**
	 * Adds a {@link Multibinder multibinder} for a given key to this module.
	 *
	 * @see ModuleBuilder#multibind
	 */
	protected final <T> void multibind(Key<T> key, Multibinder<T> multibinder) {
		checkState(builder != null, "Cannot add multibinders before or after configure() call");
		builder.multibind(key, multibinder);
	}

	protected final <V> void multibindToSet(Class<V> type) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.multibindToSet(type);
	}

	protected final <V> void multibindToSet(Class<V> type, Object qualifier) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.multibindToSet(type, qualifier);
	}

	protected final <V> void multibindToSet(Key<V> key) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.multibindToSet(key);
	}

	protected final <K, V> void multibindToMap(Class<K> keyType, Class<V> valueType) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.multibindToMap(keyType, valueType);
	}

	protected final <K, V> void multibindToMap(Class<K> keyType, Class<V> valueType, Object qualifier) {
		checkState(builder != null, "Cannot add bindings before or after configure() call");
		builder.multibindToMap(keyType, valueType, qualifier);
	}

	/**
	 * This is a helper method that provides a functionality similar to {@link ProvidesIntoSet}.
	 * It binds given binding as a singleton set to a set key made from given key
	 * and also {@link Multibinders#toSet multibinds} each of such sets together.
	 *
	 * @see ModuleBuilder#bindIntoSet(Key, Binding)
	 */
	protected final <S, T extends S> void bindIntoSet(Key<S> setOf, Binding<T> binding) {
		checkState(builder != null, "Cannot bind into set before or after configure() call");
		builder.bindIntoSet(setOf, binding);
	}

	/**
	 * This is a helper method that provides a functionality similar to {@link ProvidesIntoSet}.
	 * It binds given binding as a singleton set to a set key made from given key
	 * and also {@link Multibinders#toSet multibinds} each of such sets together.
	 *
	 * @see ModuleBuilder#bindIntoSet(Key, Binding)
	 */
	protected final <S, T extends S> void bindIntoSet(Key<S> setOf, Key<T> item) {
		bindIntoSet(setOf, Binding.to(item));
	}

	/**
	 * This is a helper method that provides a functionality similar to {@link ProvidesIntoSet}.
	 * It binds given binding as a singleton set to a set key made from given key
	 * and also {@link Multibinders#toSet multibinds} each of such sets together.
	 *
	 * @see ModuleBuilder#bindIntoSet(Key, Binding)
	 */
	protected final <S, T extends S> void bindIntoSet(@NotNull Key<S> setOf, @NotNull T element) {
		bindIntoSet(setOf, Binding.toInstance(element));
	}

	protected final void scan(Object object) {
		checkState(builder != null, "Cannot add declarative bindings before or after configure() call");
		builder.scan(object);
	}

	protected final void scan(Class<?> cls) {
		checkState(builder != null, "Cannot add declarative bindings before or after configure() call");
		builder.scan(cls);
	}

	private void finish() {
		if (builder == null) {
			return;
		}

		builder.scan(getClass().getSuperclass(), this);
		ReflectionUtils.scanClassInto(getClass(), this, builder); // so that provider methods and dsl bindings are in one 'export area'

		configure();

		Module module = builder.build();
		builder = null;

		bindings = module.getBindings();
		bindingTransformers = module.getBindingTransformers();
		bindingGenerators = module.getBindingGenerators();
		multibinders = module.getMultibinders();
	}

	@Override
	public final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings() {
		finish();
		return bindings;
	}

	@Override
	public final Map<KeyPattern<?>, Set<BindingTransformer<?>>> getBindingTransformers() {
		finish();
		return bindingTransformers;
	}

	@Override
	public final Map<KeyPattern<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
		finish();
		return bindingGenerators;
	}

	@Override
	public final Map<Key<?>, Multibinder<?>> getMultibinders() {
		finish();
		return multibinders;
	}

	// region forbid overriding default module methods

	@Override
	public Module combineWith(Module another) {
		return Module.super.combineWith(another);
	}

	@Override
	public Module overrideWith(Module another) {
		return Module.super.overrideWith(another);
	}

	@Override
	public Module transformWith(UnaryOperator<Module> fn) {
		return Module.super.transformWith(fn);
	}

	// endregion

	private String getName() {
		Class<?> cls = getClass();
		return ReflectionUtils.getDisplayName(cls.isAnonymousClass() ? cls.getGenericSuperclass() : cls);
	}

	@Override
	public String toString() {
		return getName() + "(at " + (location != null ? location : "<unknown module location>") + ')';
	}
}
