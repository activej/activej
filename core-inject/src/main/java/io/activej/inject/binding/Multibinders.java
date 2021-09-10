package io.activej.inject.binding;

import io.activej.inject.Key;
import io.activej.inject.impl.AbstractCompiledBinding;
import io.activej.inject.impl.CompiledBinding;
import io.activej.inject.impl.CompiledBindingLocator;
import io.activej.inject.util.Utils;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.activej.inject.binding.BindingType.TRANSIENT;
import static java.util.stream.Collectors.joining;

@SuppressWarnings("rawtypes")
public final class Multibinders {
	private static final Multibinder<Object> ERROR_ON_DUPLICATE = (key, bindings) -> {
		throw new DIException(bindings.stream()
				.map(Utils::getLocation)
				.collect(joining("\n\t", "Duplicate bindings for key " + key.getDisplayString() + ":\n\t", "\n")));
	};
	private static final Multibinder<Set<Object>> TO_SET = ofReducer((key, stream) -> {
		Set<Object> result = new HashSet<>();
		stream.forEach(result::addAll);
		return result;
	});
	private static final Multibinder<Map<Object, Object>> TO_MAP = ofReducer((key, stream) -> {
		Map<Object, Object> result = new HashMap<>();
		stream.forEach(map ->
				map.forEach((k, v) ->
						result.merge(k, v, ($, $2) -> {
							throw new DIException("Duplicate key " + k + " while merging maps for key " + key.getDisplayString());
						})));
		return result;
	});

	/**
	 * Default multibinder that just throws an exception if there is more than one binding per key.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Multibinder<T> errorOnDuplicate() {
		return (Multibinder<T>) ERROR_ON_DUPLICATE;
	}

	/**
	 * Multibinder that returns a binding that applies given reducing function to set of <b>instances</b> provided by all conflicting bindings.
	 */
	@SuppressWarnings("rawtypes")
	public static <T> Multibinder<T> ofReducer(BiFunction<Key<T>, Stream<T>, T> reducerFunction) {
		return (key, bindings) ->
				new Binding<T>(bindings.stream().map(Binding::getDependencies).flatMap(Collection::stream).collect(Collectors.toSet())) {
					@Override
					public CompiledBinding<T> compile(CompiledBindingLocator compiledBindingsLocator, boolean threadsafe, int scope, @Nullable Integer slot) {
						final CompiledBinding[] compiledBindings = bindings.stream()
								.map(binding -> binding.compile(compiledBindingsLocator, true, scope, null))
								.toArray(CompiledBinding[]::new);

						//noinspection Convert2Lambda
						return slot == null || bindings.stream().anyMatch(b -> b.getType() == TRANSIENT) ?
								new CompiledBinding<T>() {
									@SuppressWarnings("unchecked")
									@Override
									public T getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
										return reducerFunction.apply(key, Arrays.stream(compiledBindings)
												.map(binding -> (T) binding.getInstance(scopedInstances, synchronizedScope)));
									}
								} :
								new AbstractCompiledBinding<T>(scope, slot) {
									@SuppressWarnings("unchecked")
									@Override
									protected T doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
										return reducerFunction.apply(key, Arrays.stream(compiledBindings)
												.map(binding -> (T) binding.getInstance(scopedInstances, synchronizedScope)));
									}
								};
					}
				};
	}

	/**
	 * @see #ofReducer
	 */
	@SuppressWarnings("OptionalGetWithoutIsPresent")
	public static <T> Multibinder<T> ofBinaryOperator(BinaryOperator<T> binaryOperator) {
		return ofReducer(($, stream) -> stream.reduce(binaryOperator).get());
	}

	/**
	 * Multibinder that returns a binding for a merged set of sets provided by all conflicting bindings.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Multibinder<Set<T>> toSet() {
		return (Multibinder) TO_SET;
	}

	/**
	 * Multibinder that returns a binding for a merged map of maps provided by all conflicting bindings.
	 *
	 * @throws DIException on map merge conflicts
	 */
	@SuppressWarnings("unchecked")
	public static <K, V> Multibinder<Map<K, V>> toMap() {
		return (Multibinder) TO_MAP;
	}

	/**
	 * Combines all multibinders into one by their type and returns universal multibinder for any key from the map, falling back
	 * to {@link #errorOnDuplicate()} when map contains no multibinder for a given key.
	 */
	@SuppressWarnings("unchecked")
	public static Multibinder<?> combinedMultibinder(Map<Key<?>, Multibinder<?>> multibinders) {
		return (key, bindings) ->
				((Multibinder<Object>) multibinders.getOrDefault(key, ERROR_ON_DUPLICATE)).multibind(key, bindings);
	}
}
