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

import io.activej.inject.Key;
import io.activej.inject.KeyPattern;
import io.activej.inject.Scope;
import io.activej.inject.binding.Binding;
import io.activej.inject.binding.BindingGenerator;
import io.activej.inject.binding.BindingTransformer;
import io.activej.inject.binding.Multibinder;
import io.activej.inject.util.Trie;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

/**
 * This module provides a set of default generators.
 */
public final class DefaultModule implements Module {
	private static final Trie<Scope, Map<Key<?>, Set<Binding<?>>>> emptyTrie = Trie.leaf(new HashMap<>());
	private static final Map<KeyPattern<?>, Set<BindingGenerator<?>>> generators = new HashMap<>();

	public static synchronized <T> void register(KeyPattern<T> key, BindingGenerator<T> bindingGenerator) {
		generators.computeIfAbsent(key, $ -> new HashSet<>()).add(bindingGenerator);
	}

	@Override
	public Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getBindings() {
		return emptyTrie;
	}

	@Override
	public Map<KeyPattern<?>, Set<BindingTransformer<?>>> getBindingTransformers() {
		return emptyMap();
	}

	@Override
	public Map<KeyPattern<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
		return generators;
	}

	@Override
	public Map<Key<?>, Multibinder<?>> getMultibinders() {
		return emptyMap();
	}
}
