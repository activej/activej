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
import org.jetbrains.annotations.Nullable;

/**
 * Only reason for this not to be an anonymous class as any other in {@link Binding}
 * is that Injector does not allocate a slot for this binding
 * despite the binding being cached (so that wrappers such as mapInstance are not non-cached
 * as it would've been if the plain binding was make non-cached)
 */
public final class PlainCompiler<T> implements BindingCompiler<T> {
	private final Key<? extends T> key;

	public PlainCompiler(Key<? extends T> key) {
		this.key = key;
	}

	public Key<? extends T> getKey() {
		return key;
	}

	@SuppressWarnings("unchecked")
	@Override
	public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
		return (CompiledBinding<T>) compiledBindings.get(key);
	}
}
