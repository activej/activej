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
import io.activej.inject.impl.AbstractCompiledBinding;
import io.activej.inject.impl.CompiledBinding;
import io.activej.inject.util.Utils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.activej.inject.binding.BindingType.TRANSIENT;
import static java.util.stream.Collectors.joining;

/**
 * This is a function that is used to resolve binding conflicts.
 */
@SuppressWarnings("rawtypes")
@FunctionalInterface
public interface Multibinder<T> {
	Binding<T> multibind(Key<T> key, BindingSet<?> bindings);

	/**
	 * Default multibinder that just throws an exception if there is more than one binding per key.
	 */
	Multibinder<Object> ERROR_ON_DUPLICATE = (key, bindings) -> {
		throw new DIException(bindings.getBindings().stream()
				.map(Utils::getLocation)
				.collect(joining("\n\t", "Duplicate bindings for key " + key.getDisplayString() + ":\n\t", "\n")));
	};

	@SuppressWarnings("unchecked")
	static <T> Multibinder<T> getErrorOnDuplicate() {
		return (Multibinder<T>) ERROR_ON_DUPLICATE;
	}

	/**
	 * Multibinder that returns a binding that applies given reducing function to set of <b>instances</b> provided by all conflicting bindings.
	 */
	static <T> Multibinder<T> ofReducer(BiFunction<Key<T>, Stream<T>, T> reducerFunction) {
		return (key, bindings) ->
				new Binding<>(bindings.getBindings().stream().map(Binding::getDependencies).flatMap(Collection::stream).collect(Collectors.toSet()),
						(compiledBindings, threadsafe, scope, slot) -> {
							final CompiledBinding[] conflictedBindings = bindings.getBindings().stream()
									.map(Binding::getCompiler)
									.map(bindingCompiler -> bindingCompiler.compile(compiledBindings, true, scope, null))
									.toArray(CompiledBinding[]::new);

							//noinspection Convert2Lambda
							return slot == null || bindings.getType() == TRANSIENT ?
									new CompiledBinding<T>() {
										@SuppressWarnings("unchecked")
										@Override
										public T getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
											return reducerFunction.apply(key, Arrays.stream(conflictedBindings)
													.map(binding -> (T) binding.getInstance(scopedInstances, synchronizedScope)));
										}
									} :
									new AbstractCompiledBinding<T>(scope, slot) {
										@SuppressWarnings("unchecked")
										@Override
										protected T doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
											return reducerFunction.apply(key, Arrays.stream(conflictedBindings)
													.map(binding -> (T) binding.getInstance(scopedInstances, synchronizedScope)));
										}
									};
						});
	}

	/**
	 * @see #ofReducer
	 */
	@SuppressWarnings("OptionalGetWithoutIsPresent")
	static <T> Multibinder<T> ofBinaryOperator(BinaryOperator<T> binaryOperator) {
		return ofReducer(($, stream) -> stream.reduce(binaryOperator).get());
	}

	Multibinder<Set<Object>> TO_SET = ofReducer((key, stream) -> {
		Set<Object> result = new HashSet<>();
		stream.forEach(result::addAll);
		return result;
	});

	/**
	 * Multibinder that returns a binding for a merged set of sets provided by all conflicting bindings.
	 */
	@SuppressWarnings("unchecked")
	static <T> Multibinder<Set<T>> toSet() {
		return (Multibinder) TO_SET;
	}

	Multibinder<Map<Object, Object>> TO_MAP = ofReducer((key, stream) -> {
		Map<Object, Object> result = new HashMap<>();
		stream.forEach(map ->
				map.forEach((k, v) ->
						result.merge(k, v, ($, $2) -> {
							throw new DIException("Duplicate key " + k + " while merging maps for key " + key.getDisplayString());
						})));
		return result;
	});

	/**
	 * Multibinder that returns a binding for a merged map of maps provided by all conflicting bindings.
	 *
	 * @throws DIException on map merge conflicts
	 */
	@SuppressWarnings("unchecked")
	static <K, V> Multibinder<Map<K, V>> toMap() {
		return (Multibinder) TO_MAP;
	}

	/**
	 * Combines all multibinders into one by their type and returns universal multibinder for any key from the map, falling back
	 * to {@link #ERROR_ON_DUPLICATE} when map contains no multibinder for a given key.
	 */
	@SuppressWarnings("unchecked")
	static Multibinder<?> combinedMultibinder(Map<Key<?>, Multibinder<?>> multibinders) {
		return (key, bindings) ->
				((Multibinder<Object>) multibinders.getOrDefault(key, ERROR_ON_DUPLICATE)).multibind(key, bindings);
	}
}
