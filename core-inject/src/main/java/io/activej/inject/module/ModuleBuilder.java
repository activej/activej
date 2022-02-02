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
import io.activej.inject.KeyPattern;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.binding.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.activej.types.Types.parameterizedType;

/**
 * This interface is used to restrict the DSL.
 * Basically, it disallows any methods from {@link ModuleBuilder0} not listed below
 * to be called without previously calling {@link #bind bind(...)}.
 */
@SuppressWarnings("UnusedReturnValue")
public interface ModuleBuilder {
	static ModuleBuilder create() {
		return new ModuleBuilderImpl<>();
	}

	/**
	 * Adds all bindings, transformers, generators and multibinders from given modules to this one.
	 * <p>
	 * This works just as if you'd define all of those directly in this module.
	 */
	ModuleBuilder install(Collection<Module> modules);

	/**
	 * Adds all bindings, transformers, generators and multibinders from given modules to this one.
	 * <p>
	 * This works just as if you'd define all of those directly in this module.
	 *
	 * @see #install(Collection)
	 */
	default ModuleBuilder install(Module... modules) {
		return install(List.of(modules));
	}

	/**
	 * Scans class hierarchy and then installs providers from each class as modules,
	 * so that exports do not interfere between classes.
	 * Class parameter is used to specify from which class in the hierarchy to start.
	 */
	ModuleBuilder scan(@NotNull Class<?> containerClass, @Nullable Object container);

	/**
	 * Same as {@link #scan}, with staring class defaulting to the class of the object instance.
	 */
	default ModuleBuilder scan(Object container) {
		return scan(container.getClass(), container);
	}

	/**
	 * Same as {@link #scan}, but scans only static methods and does not depend on instance of the class.
	 * Non-static annotated methods are {@link IllegalStateException prohibited}.
	 */
	default ModuleBuilder scan(Class<?> container) {
		return scan(container, null);
	}

	/**
	 * Begins a chain of binding builder DSL calls
	 *
	 * @see #bind(Key)
	 */
	default <T> ModuleBuilder0<T> bind(Class<T> cls) {
		return bind(Key.of(cls));
	}

	/**
	 * Begins a chain of binding builder DSL calls
	 *
	 * @see #bind(Key)
	 */
	default <T> ModuleBuilder0<T> bind(Class<T> cls, Object qualifier) {
		return bind(Key.of(cls, qualifier));
	}

	/**
	 * This method begins a chain of binding builder DSL calls.
	 * <p>
	 * You can use generics in it, only those that are defined at the module class.
	 * And you need to subclass the module at the usage point to 'bake' those generics
	 * into subclass bytecode so that they could be fetched by this bind call.
	 */
	<T> ModuleBuilder0<T> bind(@NotNull Key<T> key);

	default <T> ModuleBuilder bindInstanceProvider(Class<T> type) {
		return bindInstanceProvider(Key.of(type));
	}

	default <T> ModuleBuilder bindInstanceProvider(Key<T> key) {
		return bind(Key.ofType(parameterizedType(InstanceProvider.class, key.getType()), key.getQualifier()));
	}

	default <T> ModuleBuilder bindInstanceInjector(Class<T> type) {
		return bindInstanceInjector(Key.of(type));
	}

	default <T> ModuleBuilder bindInstanceInjector(Key<T> key) {
		return bind(Key.ofType(parameterizedType(InstanceInjector.class, key.getType()), key.getQualifier()));
	}

	default <T> ModuleBuilder bindOptionalDependency(Class<T> type) {
		return bindOptionalDependency(Key.of(type));
	}

	default <T> ModuleBuilder bindOptionalDependency(Key<T> key) {
		return bind(Key.ofType(parameterizedType(OptionalDependency.class, key.getType()), key.getQualifier()));
	}

	/**
	 * This is a helper method that provides a functionality similar to {@link ProvidesIntoSet}.
	 * It binds given binding as a singleton set to a set key made from given key
	 * and also {@link Multibinders#toSet multibinds} each of such sets together.
	 */
	<S, T extends S> ModuleBuilder bindIntoSet(Key<S> setOf, Binding<T> binding);

	/**
	 * A helper method that provides a functionality similar to {@link ProvidesIntoSet}.
	 *
	 * @see #bindIntoSet(Key, Binding)
	 * @see Binding#to(Key)
	 */
	default <S, T extends S> ModuleBuilder bindIntoSet(Key<S> setOf, Key<T> item) {
		return bindIntoSet(setOf, Binding.to(item));
	}

	/**
	 * Adds a {@link BindingGenerator generator} for a given class to this module.
	 */
	<T> ModuleBuilder generate(KeyPattern<T> pattern, BindingGenerator<T> bindingGenerator);

	/**
	 * Adds a {@link BindingGenerator generator} for a given class to this module.
	 */
	default <T> ModuleBuilder generate(Class<T> pattern, BindingGenerator<T> bindingGenerator) {
		return generate(KeyPattern.of(pattern), bindingGenerator);
	}

	/**
	 * Adds a {@link BindingTransformer transformer} with a given type to this module.
	 */
	<T> ModuleBuilder transform(KeyPattern<T> pattern, BindingTransformer<T> bindingTransformer);

	/**
	 * Adds a {@link BindingTransformer transformer} with a given class to this module.
	 */
	default <T> ModuleBuilder transform(Class<T> pattern, BindingTransformer<T> bindingTransformer) {
		return transform(KeyPattern.of(pattern), bindingTransformer);
	}

	/**
	 * Adds a {@link Multibinder multibinder} for a given key to this module.
	 */
	<T> ModuleBuilder multibind(Key<T> key, Multibinder<T> multibinder);

	default <V> ModuleBuilder multibindToSet(Class<V> type) {
		return multibindToSet(Key.of(type));
	}

	default <V> ModuleBuilder multibindToSet(Class<V> type, Object qualifier) {
		return multibindToSet(Key.of(type, qualifier));
	}

	default <V> ModuleBuilder multibindToSet(Key<V> key) {
		return multibind(Key.ofType(parameterizedType(Set.class, key.getType()), key.getQualifier()), Multibinders.toSet());
	}

	default <K, V> ModuleBuilder multibindToMap(Class<K> keyType, Class<V> valueType) {
		return multibindToMap(keyType, valueType, null);
	}

	default <K, V> ModuleBuilder multibindToMap(Class<K> keyType, Class<V> valueType, Object qualifier) {
		return multibind(Key.ofType(parameterizedType(Map.class, keyType, valueType), qualifier), Multibinders.toMap());
	}

	Module build();
}
