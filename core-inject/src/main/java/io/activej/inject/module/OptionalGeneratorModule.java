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

import io.activej.inject.binding.Binding;

import java.util.Optional;

/**
 * Extension module.
 * <p>
 * A binding of <code>Optional&lt;T&gt;</code> for any type <code>T</code> is generated,
 * with the resulting optional being empty if no binding for <code>T</code> was bound
 * or containing an instance of <code>T</code>
 */
public final class OptionalGeneratorModule extends AbstractModule {
	private OptionalGeneratorModule() {
	}

	public static OptionalGeneratorModule create() {
		return new OptionalGeneratorModule();
	}

	@Override
	protected void configure() {
		generate(Optional.class, (bindings, scope, key) -> {
			Binding<?> binding = bindings.get(key.getTypeParameter(0));
			return binding != null ?
					binding.mapInstance(Optional::of) :
					Binding.toInstance(Optional.empty());
		});
	}
}
