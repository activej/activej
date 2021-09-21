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
import io.activej.inject.binding.BindingGenerator;
import org.jetbrains.annotations.Nullable;

/**
 * This function is passed to a {@link BindingGenerator generator} when trying to generate a binding.
 * <p>
 * Generators can depend on other bindings that could not be present but can be generated.
 * This function is used as a mean of recursion - when no requested binding is present it tries to generate it,
 * and it is called from the generator itself.
 */
@FunctionalInterface
public interface BindingLocator {
	/**
	 * Retrieves existing binding for given key or tries to recursively generate it from known {@link BindingGenerator generators}.
	 */
	<T> @Nullable Binding<T> get(Key<T> key);
}
