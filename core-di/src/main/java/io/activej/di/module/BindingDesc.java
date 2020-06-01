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

package io.activej.di.module;

import io.activej.di.Key;
import io.activej.di.Scope;
import io.activej.di.binding.Binding;
import io.activej.di.binding.BindingType;

import static io.activej.di.Scope.UNSCOPED;
import static io.activej.di.binding.BindingType.REGULAR;

public final class BindingDesc {
	private Key<?> key;
	private Binding<?> binding;
	private Scope[] scope;
	private BindingType type;

	public BindingDesc(Key<?> key, Binding<?> binding) {
		this.key = key;
		this.binding = binding;
		this.scope = UNSCOPED;
		this.type = REGULAR;
	}

	public Key<?> getKey() {
		return key;
	}

	public void setKey(Key<?> key) {
		this.key = key;
	}

	public Binding<?> getBinding() {
		return binding;
	}

	public void setBinding(Binding<?> binding) {
		this.binding = binding;
	}

	public Scope[] getScope() {
		return scope;
	}

	public void setScope(Scope[] scope) {
		this.scope = scope;
	}

	public BindingType getType() {
		return type;
	}

	public void setType(BindingType type) {
		this.type = type;
	}
}
