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

/**
 * This is a transformation function that is applied by the {@link Injector injector} to each binding once.
 */
@FunctionalInterface
public interface BindingTransformer<T> {
	@NotNull Binding<T> transform(BindingLocator bindings, Scope[] scope, Key<T> key, Binding<T> binding);
}
