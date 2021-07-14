package io.activej.inject.binding;

import io.activej.inject.Injector;
import io.activej.inject.Scope;
import io.activej.inject.util.TypeUtils;
import io.activej.inject.util.Utils;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static io.activej.inject.util.Utils.sortPatternsMap;
import static java.util.stream.Collectors.toMap;

public final class BindingTransformers {
	private static final BindingTransformer<Object> IDENTITY = (bindings, scope, key, binding) -> binding;

	@SuppressWarnings("unchecked")
	public static <T> BindingTransformer<T> identity() {
		return (BindingTransformer<T>) IDENTITY;
	}

	/**
	 * Modules export a priority multimap of transformers.
	 * <p>
	 * This transformer aggregates such map into one big generator to be used by {@link Injector#compile} method.
	 * The map is converted to a sorted list of sets.
	 * Then for each of those sets, similar to {@link BindingGenerators#combinedGenerator generators},
	 * only zero or one transformer from that set are allowed to return anything but the binding is was given (being an identity transformer).
	 * <p>
	 * So if two transformers differ in priority then they can be applied both in order of their priority.
	 *
	 * @param transformers
	 */
	@SuppressWarnings("unchecked")
	public static BindingTransformer<?> combinedTransformer(Map<Type, Set<BindingTransformer<?>>> transformers) {
		LinkedHashMap<Type, Set<BindingTransformer<?>>> sorted = sortPatternsMap(transformers);
		return (bindings, scope, key, binding) -> {
			Binding<Object> result = binding;
			for (Map.Entry<Type, Set<BindingTransformer<?>>> entry : sorted.entrySet()) {
				if (TypeUtils.isAssignable(entry.getKey(), key.getType())) {
					for (BindingTransformer<?> transformer : entry.getValue()) {
						result = ((BindingTransformer<Object>) transformer).transform(bindings, scope, key, result);
					}
				}
			}
			return result;
		};
	}
}
