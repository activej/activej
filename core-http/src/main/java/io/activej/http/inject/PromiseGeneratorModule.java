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

package io.activej.http.inject;

import io.activej.inject.binding.Binding;
import io.activej.inject.binding.BindingGenerator;
import io.activej.inject.module.AbstractModule;
import io.activej.promise.Promise;

/**
 * @since 3.0.0
 */
public class PromiseGeneratorModule extends AbstractModule {
	private PromiseGeneratorModule() {
	}

	public static PromiseGeneratorModule create() {
		return new PromiseGeneratorModule();
	}

	@Override
	protected void configure() {
		BindingGenerator<Promise<?>> generator = (bindings, scope, key) -> {
			Binding<Object> binding = bindings.get(key.getTypeParameter(0));
			return binding != null ?
					binding.mapInstance(Promise::of) :
					null;
		};
		generate(Promise.class, generator);
	}
}
