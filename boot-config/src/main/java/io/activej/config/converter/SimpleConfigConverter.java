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

package io.activej.config.converter;

import io.activej.config.Config;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.Checks.checkNotNull;

public abstract class SimpleConfigConverter<T> implements ConfigConverter<T> {
	@NotNull
	@Override
	public final T get(Config config) {
		return fromString(checkNotNull(config.getValue()));
	}

	@Override
	public final @Nullable T get(Config config, @Nullable T defaultValue) {
		String value = config.getValue(defaultValue == null ? "" : toString(defaultValue));
		return value == null || value.trim().isEmpty() ? defaultValue : fromString(value);
	}

	protected abstract T fromString(@Nullable String string);

	protected abstract String toString(T value);
}
