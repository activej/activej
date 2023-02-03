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

package io.activej.aggregation.fieldtype;

import io.activej.aggregation.util.HyperLogLog;
import io.activej.aggregation.util.JsonCodec;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.common.annotation.StaticFactories;
import io.activej.serializer.StringFormat;
import io.activej.serializer.def.PrimitiveSerializerDef;
import io.activej.serializer.def.SerializerDef;
import io.activej.serializer.def.SerializerDefs;
import io.activej.serializer.def.impl.*;
import io.activej.types.Primitives;
import io.activej.types.Types;

import java.lang.reflect.Type;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.StringFormat.UTF8;
import static java.time.temporal.ChronoUnit.DAYS;

@StaticFactories(FieldType.class)
public final class FieldTypes {

	public static FieldType<Byte> ofByte() {
		return new FieldType<>(byte.class, SerializerDefs.ofByte(false), JsonCodecs.ofByte()) {
			@Override
			public Expression toStringValue(Expression value) {
				return Expressions.staticCall(Byte.class, "toString", value);
			}
		};
	}

	public static FieldType<Short> ofShort() {
		return new FieldType<>(short.class, SerializerDefs.ofShort(false), JsonCodecs.ofShort()) {
			@Override
			public Expression toStringValue(Expression value) {
				return Expressions.staticCall(Short.class, "toString", value);
			}
		};
	}

	public static FieldType<Integer> ofInt() {
		return new FieldType<>(int.class, SerializerDefs.ofInt(false, true), JsonCodecs.ofInteger());
	}

	public static FieldType<Long> ofLong() {
		return new FieldType<>(long.class, SerializerDefs.ofLong(false, true), JsonCodecs.ofLong());
	}

	public static FieldType<Float> ofFloat() {
		return new FieldType<>(float.class, SerializerDefs.ofFloat(false), JsonCodecs.ofFloat());
	}

	public static FieldType<Double> ofDouble() {
		return new FieldType<>(double.class, SerializerDefs.ofDouble(false), JsonCodecs.ofDouble());
	}

	public static FieldType<Character> ofChar() {
		return new FieldType<>(char.class, SerializerDefs.ofChar(false), JsonCodecs.ofCharacter());
	}

	public static FieldType<Boolean> ofBoolean() {
		return new FieldType<>(boolean.class, SerializerDefs.ofBoolean(false), JsonCodecs.ofBoolean());
	}

	public static FieldType<Integer> ofHyperLogLog() {
		return new FieldType<>(HyperLogLog.class, int.class, serializerDefHyperLogLog(), JsonCodecs.ofInteger(), null);
	}

	private static SerializerDef serializerDefHyperLogLog() {
		try {
			return ClassDef.builder(HyperLogLog.class)
					.withGetter(HyperLogLog.class.getMethod("getRegisters"),
							SerializerDefs.ofArray(SerializerDefs.ofByte(false), byte[].class), -1, -1)
					.withConstructor(HyperLogLog.class.getConstructor(byte[].class),
							List.of("registers"))
					.build();
		} catch (NoSuchMethodException e) {
			throw new AssertionError(e);
		}
	}

	public static <T> FieldType<Set<T>> ofSet(FieldType<T> fieldType) {
		SerializerDef itemSerializer = fieldType.getSerializer();
		if (itemSerializer instanceof PrimitiveSerializerDef) {
			itemSerializer = ((PrimitiveSerializerDef) itemSerializer).ensureWrapped();
		}
		SerializerDef serializer = SerializerDefs.ofSet(itemSerializer);
		Type wrappedNestedType = fieldType.getDataType() instanceof Class ?
				Primitives.wrap((Class<?>) fieldType.getDataType()) :
				fieldType.getDataType();
		Type dataType = Types.parameterizedType(Set.class, wrappedNestedType);
		JsonCodec<Set<T>> codec = JsonCodecs.ofSet(fieldType.getCodec());
		return new FieldType<>(Set.class, dataType, serializer, codec, codec);
	}

	public static <E extends Enum<E>> FieldType<E> ofEnum(Class<E> enumClass) {
		return new FieldType<>(enumClass, SerializerDefs.ofEnum(enumClass), JsonCodecs.ofEnum(enumClass));
	}

	public static FieldType<String> ofString() {
		return ofString(UTF8);
	}

	public static FieldType<String> ofString(StringFormat format) {
		return new FieldType<>(String.class, SerializerDefs.ofString(format), JsonCodecs.ofString()) {
			@Override
			public Expression toStringValue(Expression value) {
				return value;
			}
		};
	}

	public static FieldType<LocalDate> ofLocalDate() {
		return ofLocalDate(LocalDate.parse("1970-01-01"));
	}

	public static FieldType<LocalDate> ofLocalDate(LocalDate startDate) {
		return new FieldType<>(int.class, LocalDate.class, SerializerDefs.ofInt(false, true), JsonCodecs.ofLocalDate(), JsonCodecs.ofInteger()) {

			@Override
			public Expression toValue(Expression internalValue) {
				return call(value(startDate), "plusDays", cast(internalValue, long.class));
			}

			@Override
			public LocalDate toInitialValue(Object internalValue) {
				return startDate.plusDays((int) internalValue);
			}

			@Override
			public Object toInternalValue(LocalDate value) {
				return (int) DAYS.between(startDate, value);
			}
		};
	}
}
