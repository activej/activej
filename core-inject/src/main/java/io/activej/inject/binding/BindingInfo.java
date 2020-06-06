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

import io.activej.inject.util.LocationInfo;
import io.activej.inject.util.MarkedBinding;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

import static io.activej.inject.binding.BindingType.EAGER;
import static io.activej.inject.binding.BindingType.TRANSIENT;

public final class BindingInfo {
	private final Set<Dependency> dependencies;
	private final BindingType type;

	@Nullable
	private final LocationInfo location;

	public BindingInfo(Set<Dependency> dependencies, BindingType type, @Nullable LocationInfo location) {
		this.dependencies = dependencies;
		this.type = type;
		this.location = location;
	}

	public static BindingInfo from(MarkedBinding<?> markedBinding) {
		Binding<?> binding = markedBinding.getBinding();
		return new BindingInfo(binding.getDependencies(), markedBinding.getType(), binding.getLocation());
	}

	@NotNull
	public Set<Dependency> getDependencies() {
		return dependencies;
	}

	public BindingType getType() {
		return type;
	}

	@Nullable
	public LocationInfo getLocation() {
		return location;
	}

	@Override
	public String toString() {
		return (type == TRANSIENT ? "*" : type == EAGER ? "!" : "") + "Binding" + dependencies;
	}
}
