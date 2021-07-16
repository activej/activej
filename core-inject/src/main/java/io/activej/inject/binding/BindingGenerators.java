package io.activej.inject.binding;

import io.activej.inject.Injector;
import io.activej.inject.KeyPattern;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static io.activej.inject.util.Utils.sortPatternsMap;

public final class BindingGenerators {
	private static final BindingGenerator<Object> REFUSING = (bindings, scope, key) -> null;

	/**
	 * Default generator that never generates anything.
	 */
	@SuppressWarnings("unchecked")
	public static <T> BindingGenerator<T> refusing() {
		return (BindingGenerator<T>) REFUSING;
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
	 */
	@SuppressWarnings("unchecked")
	public static BindingGenerator<?> combinedGenerator(Map<KeyPattern<?>, Set<BindingGenerator<?>>> generators) {
		LinkedHashMap<KeyPattern<?>, Set<BindingGenerator<?>>> sorted = sortPatternsMap(generators);
		return (bindings, scope, key) -> {
			for (Map.Entry<KeyPattern<?>, Set<BindingGenerator<?>>> entry : sorted.entrySet()) {
				if (entry.getKey().match(key)) {
					for (BindingGenerator<?> generator : entry.getValue()) {
						@Nullable Binding<Object> generated = ((BindingGenerator<Object>) generator).generate(bindings, scope, key);
						if (generated != null) return generated;
					}
				}
			}
			return null;
		};
	}

}
