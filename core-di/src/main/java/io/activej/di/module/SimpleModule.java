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
import io.activej.di.binding.BindingGenerator;
import io.activej.di.binding.BindingSet;
import io.activej.di.binding.BindingTransformer;
import io.activej.di.binding.Multibinder;
import io.activej.di.util.Trie;

import java.util.Map;
import java.util.Set;

final class SimpleModule implements Module {
	private final Trie<Scope, Map<Key<?>, BindingSet<?>>> bindings;
	private final Map<Integer, Set<BindingTransformer<?>>> transformers;
	private final Map<Class<?>, Set<BindingGenerator<?>>> generators;
	private final Map<Key<?>, Multibinder<?>> multibinders;

	public SimpleModule(Trie<Scope, Map<Key<?>, BindingSet<?>>> bindings,
			Map<Integer, Set<BindingTransformer<?>>> transformers,
			Map<Class<?>, Set<BindingGenerator<?>>> generators,
			Map<Key<?>, Multibinder<?>> multibinders) {
		this.bindings = bindings;
		this.transformers = transformers;
		this.generators = generators;
		this.multibinders = multibinders;
	}

	@Override
	public Trie<Scope, Map<Key<?>, BindingSet<?>>> getBindings() {
		return bindings;
	}

	@Override
	public Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers() {
		return transformers;
	}

	@Override
	public Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
		return generators;
	}

	@Override
	public Map<Key<?>, Multibinder<?>> getMultibinders() {
		return multibinders;
	}
}
