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

package io.activej.di.util;

import io.activej.di.binding.Binding;
import io.activej.di.binding.BindingType;

import static io.activej.di.binding.BindingType.EAGER;
import static io.activej.di.binding.BindingType.TRANSIENT;

/**
 * A container that groups together a bindings and its type, only for internal use
 */
public final class MarkedBinding<K> {
	private final Binding<K> binding;
	private final BindingType type;

	public MarkedBinding(Binding<K> binding, BindingType type) {
		this.binding = binding;
		this.type = type;
	}

	public Binding<K> getBinding() {
		return binding;
	}

	public BindingType getType() {
		return type;
	}

	@Override
	public String toString() {
		return (type == TRANSIENT ? "*" : type == EAGER ? "!" : "") + binding.toString();
	}
}
