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

package io.activej.inject.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Method;

/**
 * LocationInfo is a transient field in {@link io.activej.inject.binding.Binding binding} that is set
 * where possible by the DSL so that error messages can show where a binding was made.
 */
public final class LocationInfo {
	private final Object module;
	private final @Nullable Method provider;

	private LocationInfo(Object module, @Nullable Method provider) {
		this.module = module;
		this.provider = provider;
	}

	public static LocationInfo from(@NotNull Object module, @NotNull Method provider) {
		return new LocationInfo(module, provider);
	}

	public static LocationInfo from(@NotNull Object module) {
		return new LocationInfo(module, null);
	}

	public @NotNull Object getModule() {
		return module;
	}

	public @Nullable Method getProvider() {
		return provider;
	}

	@Override
	public String toString() {
		if (provider == null) {
			return "module " + module;
		}
		String shortName = ReflectionUtils.getShortName(provider.getDeclaringClass());
		return "object " + module + ", provider method " + shortName + "." + provider.getName() + "(" + shortName + ".java:0)";
	}
}
