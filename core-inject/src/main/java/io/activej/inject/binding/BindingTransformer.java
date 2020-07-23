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

import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.Scope;
import io.activej.inject.impl.BindingLocator;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.util.stream.Collectors.toList;

/**
 * This is a transformation function that is applied by the {@link Injector injector} to each binding once.
 */
@FunctionalInterface
public interface BindingTransformer<T> {
	@NotNull Binding<T> transform(BindingLocator bindings, Scope[] scope, Key<T> key, Binding<T> binding);

	static <T> BindingTransformer<T> identity() {
		return (bindings, scope, key, binding) -> binding;
	}

	/**
	 * Modules export a priority multimap of transformers.
	 * <p>
	 * This transformer aggregates such map into one big generator to be used by {@link Injector#compile} method.
	 * The map is converted to a sorted list of sets.
	 * Then for each of those sets, similar to {@link BindingGenerator#combinedGenerator generators},
	 * only zero or one transformer from that set are allowed to return anything but the binding is was given (being an identity transformer).
	 * <p>
	 * So if two transformers differ in priority then they can be applied both in order of their priority.
	 */
	@SuppressWarnings("unchecked")
	static BindingTransformer<?> combinedTransformer(Map<Integer, Set<BindingTransformer<?>>> transformers) {

		List<Set<BindingTransformer<?>>> transformerList = transformers.entrySet().stream()
				.sorted(Entry.comparingByKey())
				.map(Entry::getValue)
				.collect(toList());

		return (bindings, scope, key, binding) -> {
			Binding<Object> result = binding;

			for (Set<BindingTransformer<?>> localTransformers : transformerList) {

				Binding<Object> transformed = null;

				for (BindingTransformer<?> transformer : localTransformers) {
					Binding<Object> b = ((BindingTransformer<Object>) transformer).transform(bindings, scope, key, result);
					if (b.equals(binding)) {
						continue;
					}
					if (transformed != null) {
						throw new DIException("More than one transformer with the same priority transformed a binding for key " + key.getDisplayString());
					}
					transformed = b;
				}
				if (transformed != null) {
					result = transformed;
				}
			}
			return result;
		};
	}
}
