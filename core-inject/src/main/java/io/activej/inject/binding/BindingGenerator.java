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

import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.Scope;
import io.activej.inject.impl.BindingLocator;
import io.activej.inject.util.Types;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * This is a function that can try to generate a missing dependency binding when
 * {@link Injector} compiles the final binding graph trie.
 * <p>
 * An example of such function could be {@link io.activej.inject.util.ReflectionUtils#generateImplicitBinding the injection DSL}.
 *
 * @see Injector#compile
 */
@FunctionalInterface
public interface BindingGenerator<T> {
	@Nullable Binding<T> generate(BindingLocator bindings, Scope[] scope, Key<T> key);

	/**
	 * Default generator that never generates anything.
	 */
	static <T> BindingGenerator<T> refusing() {
		return (bindings, scope, key) -> null;
	}

	/**
	 * Modules export a multimap of binding generators with raw class keys.
	 * <p>
	 * This generator aggregates such map into one big generator to be used by {@link Injector#compile} method.
	 * For each requested key, a set of generators is fetched based on its raw class.
	 * All generators from this set are then called and if one and only one of them generated a binding then this
	 * binding is returned.
	 * When nobody generated a binding then combined generator didn't as well, and it is considered an error when more than one did.
	 * <p>
	 * When raw class of the requested key is an interface, it is matched by equality, and when it is some class then generator of its closest superclass
	 * will be called.
	 * <p>
	 * Be aware of that when creating generators for some concrete classes: usually they are made for interfaces or for final classes.
	 *
	 * @throws DIException when more than one generator provides a binding for given key
	 */
	@SuppressWarnings("unchecked")
	static BindingGenerator<?> combinedGenerator(Map<Class<?>, Set<BindingGenerator<?>>> generators) {
		return (bindings, scope, key) -> {
			Class<Object> rawType = key.getRawType();
			Class<?> generatorKey = rawType.isInterface() ? rawType : Types.findClosestAncestor(rawType, generators.keySet());
			if (generatorKey == null) {
				return null;
			}
			Set<BindingGenerator<?>> found = generators.get(generatorKey);
			if (found == null) {
				return null;
			}

			Set<Binding<Object>> generatedBindings = found.stream()
					.map(generator -> ((BindingGenerator<Object>) generator).generate(bindings, scope, key))
					.filter(Objects::nonNull)
					.collect(toSet());

			switch (generatedBindings.size()) {
				case 0:
					return null;
				case 1:
					return generatedBindings.iterator().next();
				default:
					throw new DIException("More than one generator provided a binding for key " + key.getDisplayString());
			}
		};
	}
}
