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
import io.activej.inject.Scope;
import io.activej.inject.binding.*;
import io.activej.inject.impl.Preprocessor;
import io.activej.inject.util.Trie;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.UnaryOperator;

import static io.activej.inject.binding.BindingGenerators.combinedGenerator;
import static io.activej.inject.binding.BindingTransformers.combinedTransformer;
import static io.activej.inject.binding.Multibinders.combinedMultibinder;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

/**
 * A module is an object, that provides certain sets of bindings, transformers, generators or multibinders
 * arranged by keys in certain data structures.
 *
 * @see AbstractModule
 */
public interface Module {
	Trie<Scope, Map<Key<?>, BindingSet<?>>> getBindings();

	Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers();

	Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators();

	Map<Key<?>, Multibinder<?>> getMultibinders();

	default Module combineWith(Module another) {
		return Modules.combine(this, another);
	}

	default Module overrideWith(Module another) {
		return Modules.override(this, another);
	}

	default Module transformWith(UnaryOperator<Module> fn) {
		return fn.apply(this);
	}

	/**
	 * A shortcut that reduces bindings multimap trie from this module using multibinders, transformers and generators from this module.
	 * <p>
	 * Note that this method expensive to call repeatedly
	 */
	default Trie<Scope, Map<Key<?>, BindingInfo>> getReducedBindingInfo() {
		return Preprocessor.reduce(
				getBindings(),
				combinedMultibinder(getMultibinders()),
				combinedTransformer(getBindingTransformers()),
				combinedGenerator(getBindingGenerators()))
				.map(map -> map.entrySet().stream().collect(toMap(Entry::getKey, e -> BindingInfo.from(e.getValue()))));
	}

	/**
	 * Returns an empty {@link Module module}.
	 */
	static Module empty() {
		return Modules.EMPTY;
	}

	/**
	 * Creates a {@link Module module} out of given binding graph trie
	 */
	static Module of(Trie<Scope, Map<Key<?>, BindingSet<?>>> bindings) {
		return new SimpleModule(bindings, emptyMap(), emptyMap(), emptyMap());
	}

	/**
	 * Creates a {@link Module module} out of given binding graph trie, transformers, generators and multibinders
	 */
	static Module of(Trie<Scope, Map<Key<?>, BindingSet<?>>> bindings,
			Map<Integer, Set<BindingTransformer<?>>> transformers,
			Map<Class<?>, Set<BindingGenerator<?>>> generators,
			Map<Key<?>, Multibinder<?>> multibinders) {
		return new SimpleModule(bindings, transformers, generators, multibinders);
	}
}
