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

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public final class JmxReducers {

	public static final class JmxReducerDistinct implements JmxReducer<Object> {
		@Override
		public Object reduce(List<?> list) {
			if (list.isEmpty()) return null;
			Object firstValue = list.get(0);
			return list.stream().allMatch(value -> Objects.equals(firstValue, value)) ? firstValue : null;
		}
	}

	public static final class JmxReducerSum implements JmxReducer<Number> {
		@Override
		public Number reduce(List<? extends Number> list) {
			return reduceNumbers(list, DoubleStream::sum, LongStream::sum);
		}
	}

	public static final class JmxReducerMin implements JmxReducer<Number> {
		@Override
		public Number reduce(List<? extends Number> list) {
			return reduceNumbers(list, s -> s.min().getAsDouble(), s -> s.min().getAsLong());
		}
	}

	public static final class JmxReducerMax implements JmxReducer<Number> {
		@Override
		public Number reduce(List<? extends Number> list) {
			return reduceNumbers(list, s -> s.max().getAsDouble(), s -> s.max().getAsLong());
		}
	}

	@Nullable
	private static Number reduceNumbers(List<? extends Number> list,
			Function<DoubleStream, Double> opDouble, Function<LongStream, Long> opLong) {
		list = list.stream().filter(Objects::nonNull).collect(toList());

		if (list.isEmpty()) return null;

		Class<?> numberClass = list.get(0).getClass();
		if (isFloatingPointNumber(numberClass)) {
			return convert(numberClass,
					opDouble.apply(list.stream().mapToDouble(Number::doubleValue)));
		} else if (isIntegerNumber(numberClass)) {
			return convert(numberClass,
					opLong.apply(list.stream().mapToLong(Number::longValue)));
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

}
