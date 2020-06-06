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

package io.activej.inject;

import io.activej.inject.annotation.Transient;

/**
 * A provider, unlike other DI frameworks, is just a version of {@link Injector#getInstance} with a baked in key.
 * If you need a function that returns a new object each time then you need to make your binding {@link Transient transient}.
 * <p>
 * The main reason for its existence is that it has a {@link io.activej.inject.module.DefaultModule default generator}
 * for its binding, so it can be fluently requested by {@link io.activej.inject.annotation.Provides provider methods} etc.
 * <p>
 * Also it can be used for lazy dependency cycle resolution.
 */
public interface InstanceProvider<T> {
	Key<T> key();

	T get();
}
