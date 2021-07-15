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

import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Transient;
import io.activej.inject.binding.BindingType;

import static io.activej.inject.binding.BindingType.EAGER;
import static io.activej.inject.binding.BindingType.TRANSIENT;

public interface ModuleBuilder1<T> extends ModuleBuilder0<T> {

	ModuleBuilder1<T> as(BindingType type);

	/**
	 * Marks this binding as {@link Eager eager}.
	 * <p>
	 * Note that bindings cannot be both {@link Eager eager} and {@link Transient transient}.
	 */
	default ModuleBuilder1<T> asEager() {
		return as(EAGER);
	}

	/**
	 * Marks this binding as {@link Transient transient}.
	 */
	default ModuleBuilder1<T> asTransient() {
		return as(TRANSIENT);
	}
}
