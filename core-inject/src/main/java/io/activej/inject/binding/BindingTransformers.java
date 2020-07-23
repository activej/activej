package io.activej.inject.binding;

import io.activej.inject.Injector;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;

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
	 */
	@SuppressWarnings("unchecked")
	public static BindingTransformer<?> combinedTransformer(Map<Integer, Set<BindingTransformer<?>>> transformers) {

		List<Set<BindingTransformer<?>>> transformerList = transformers.entrySet().stream()
				.sorted(Map.Entry.comparingByKey())
				.map(Map.Entry::getValue)
				.collect(toList());

		return (bindings, scope, key, binding) -> {
			Binding<Object> result = binding;

			for (Set<BindingTransformer<?>> localTransformers : transformerList) {

				Binding<Object> transformed = null;

				for (BindingTransformer<?> transformer : localTransformers) {
					Binding<Object> b = ((BindingTransformer<Object>) transformer).transform(bindings, scope, key, result);
					if (b.equals(binding)) {
						continue;
					}
					if (transformed != null) {
						throw new DIException("More than one transformer with the same priority transformed a binding for key " + key.getDisplayString());
					}
					transformed = b;
				}
				if (transformed != null) {
					result = transformed;
				}
			}
			return result;
		};
	}
}
