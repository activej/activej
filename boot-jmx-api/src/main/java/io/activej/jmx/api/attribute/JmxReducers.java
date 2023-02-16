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

package io.activej.jmx.api.attribute;

import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;

public final class JmxReducers {

	public static final class JmxReducerDistinct implements JmxReducer<Object> {
		@Override
		public Object reduce(List<?> list) {
			if (list.isEmpty()) return null;
			Object firstValue = list.get(0);
			return list.stream().allMatch(value -> Objects.equals(firstValue, value)) ? firstValue : null;
		}
	}

	public static final class JmxReducerSum implements JmxReducer<Object> {
		@Override
		public @Nullable Object reduce(List<?> list) {
			list = list.stream().filter(Objects::nonNull).collect(toList());

			if (list.isEmpty()) return null;

			Class<?> attributeClass = list.get(0).getClass();
			if (Number.class.isAssignableFrom(attributeClass)) {
				//noinspection unchecked
				return reduceNumbers((List<? extends Number>) list, DoubleStream::sum, LongStream::sum);
			}

			if (attributeClass == Duration.class) {
				//noinspection unchecked
				return sumDuration((List<Duration>) list);
			}

			throw new IllegalStateException("Cannot sum values of type: " + attributeClass);
		}
	}

	public static final class JmxReducerAvg implements JmxReducer<Object> {
		@Override
		public @Nullable Object reduce(List<?> list) {
			list = list.stream().filter(Objects::nonNull).collect(toList());

			if (list.isEmpty()) return null;

			Class<?> attributeClass = list.get(0).getClass();
			if (Number.class.isAssignableFrom(attributeClass)) {
				//noinspection unchecked,OptionalGetWithoutIsPresent
				return reduceNumbers((List<? extends Number>) list,
						d -> d.average().getAsDouble(),
						l -> (long) l.average().getAsDouble()
				);
			}

			if (attributeClass == Duration.class) {
				//noinspection unchecked
				return sumDuration((List<Duration>) list).dividedBy(list.size());
			}

			throw new IllegalStateException("Cannot sum values of type: " + attributeClass);
		}
	}

	public static final class JmxReducerMin<C extends Comparable<C>> implements JmxReducer<C> {
		@Override
		public @Nullable C reduce(List<? extends C> list) {
			return list.stream()
					.filter(Objects::nonNull)
					.min(Comparator.naturalOrder())
					.orElse(null);
		}
	}

	public static final class JmxReducerMax<C extends Comparable<C>> implements JmxReducer<C> {
		@Override
		public @Nullable C reduce(List<? extends C> list) {
			return list.stream()
					.filter(Objects::nonNull)
					.max(Comparator.naturalOrder())
					.orElse(null);
		}
	}

	private static @Nullable Number reduceNumbers(List<? extends Number> list,
			ToDoubleFunction<DoubleStream> opDouble, ToLongFunction<LongStream> opLong) {
		Class<?> numberClass = list.get(0).getClass();
		if (isFloatingPointNumber(numberClass)) {
			return convert(numberClass,
					opDouble.applyAsDouble(list.stream().mapToDouble(Number::doubleValue)));
		} else if (isIntegerNumber(numberClass)) {
			return convert(numberClass,
					opLong.applyAsLong(list.stream().mapToLong(Number::longValue)));
		} else {
			throw new IllegalArgumentException(
					"Unsupported objects of type: " + numberClass.getName());
		}
	}

	private static boolean isFloatingPointNumber(Class<?> numberClass) {
		return Float.class.isAssignableFrom(numberClass) || Double.class.isAssignableFrom(numberClass);
	}

	private static boolean isIntegerNumber(Class<?> numberClass) {
		return Byte.class.isAssignableFrom(numberClass) || Short.class.isAssignableFrom(numberClass)
				|| Integer.class.isAssignableFrom(numberClass) || Long.class.isAssignableFrom(numberClass);
	}

	private static Number convert(Class<?> targetClass, Number number) {
		if (Byte.class.isAssignableFrom(targetClass)) {
			return number.byteValue();
		} else if (Short.class.isAssignableFrom(targetClass)) {
			return number.shortValue();
		} else if (Integer.class.isAssignableFrom(targetClass)) {
			return number.intValue();
		} else if (Long.class.isAssignableFrom(targetClass)) {
			return number.longValue();
		} else if (Float.class.isAssignableFrom(targetClass)) {
			return number.floatValue();
		} else if (Double.class.isAssignableFrom(targetClass)) {
			return number.doubleValue();
		} else {
			throw new IllegalArgumentException("target class is not a number class");
		}
	}

	private static Duration sumDuration(List<Duration> list) {
		Duration initial = Duration.ZERO;
		for (Duration o : list) {
			initial = initial.plus(o);
		}
		return initial;
	}
}
