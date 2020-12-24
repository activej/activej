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

import io.activej.codec.StructuredCodec;
import io.activej.codec.StructuredCodecs;
import io.activej.codec.StructuredInput;
import io.activej.codec.StructuredOutput;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.codegen.util.Primitives;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.reflection.RecursiveType;
import io.activej.serializer.SerializerDef;
import io.activej.serializer.StringFormat;
import io.activej.serializer.impl.*;

import java.lang.reflect.Type;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.Set;

import static io.activej.codec.StructuredCodecs.*;
import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.StringFormat.UTF8;
import static java.time.temporal.ChronoUnit.DAYS;

public final class FieldTypes {

	public static FieldType<Byte> ofByte() {
		return new FieldType<Byte>(byte.class, new SerializerDefByte(false), BYTE_CODEC) {
			@Override
			public Expression toStringValue(Expression value) {
				return Expressions.staticCall(Byte.class, "toString", value);
			}
		};
	}

	public static FieldType<Short> ofShort() {
		return new FieldType<Short>(short.class, new SerializerDefShort(false), SHORT_CODEC) {
			@Override
			public Expression toStringValue(Expression value) {
				return Expressions.staticCall(Short.class, "toString", value);
			}
		};
	}

	public static FieldType<Integer> ofInt() {
		return new FieldType<>(int.class, new SerializerDefInt(false, true), INT_CODEC);
	}

	public static FieldType<Long> ofLong() {
		return new FieldType<>(long.class, new SerializerDefLong(false, true), LONG_CODEC);
	}

	public static FieldType<Float> ofFloat() {
		return new FieldType<>(float.class, new SerializerDefFloat(false), FLOAT_CODEC);
	}

	public static FieldType<Double> ofDouble() {
		return new FieldType<>(double.class, new SerializerDefDouble(false), DOUBLE_CODEC);
	}

	public static FieldType<Character> ofChar() {
		return new FieldType<>(char.class, new SerializerDefChar(false), CHARACTER_CODEC);
	}

	public static FieldType<Boolean> ofBoolean() {
		return new FieldType<>(boolean.class, new SerializerDefBoolean(false), BOOLEAN_CODEC);
	}

	public static <T> FieldType<Set<T>> ofSet(FieldType<T> fieldType) {
		SerializerDef itemSerializer = fieldType.getSerializer();
		if (itemSerializer instanceof SerializerDefPrimitive) {
			itemSerializer = ((SerializerDefPrimitive) itemSerializer).ensureWrapped();
		}
		SerializerDefSet serializer = new SerializerDefSet(itemSerializer);
		Type wrappedNestedType = fieldType.getDataType() instanceof Class ?
				Primitives.wrap((Class<?>) fieldType.getDataType()) :
				fieldType.getDataType();
		Type dataType = RecursiveType.of(Set.class, RecursiveType.of(wrappedNestedType)).getType();
		StructuredCodec<Set<T>> codec = StructuredCodecs.ofSet(fieldType.getCodec());
		return new FieldType<>(Set.class, dataType, serializer, codec, codec);
	}

	public static <E extends Enum<E>> FieldType<E> ofEnum(Class<E> enumClass) {
		return new FieldType<>(enumClass, new SerializerDefEnum(enumClass), StructuredCodecs.ofEnum(enumClass));
	}

	public static FieldType<String> ofString() {
		return ofString(UTF8);
	}

	public static FieldType<String> ofString(StringFormat format) {
		return new FieldType<String>(String.class, new SerializerDefString(format), STRING_CODEC) {
			@Override
			public Expression toStringValue(Expression value) {
				return value;
			}
		};
	}

	public static FieldType<LocalDate> ofLocalDate() {
		return new FieldTypeDate();
	}

	public static FieldType<LocalDate> ofLocalDate(LocalDate startDate) {
		return new FieldTypeDate(startDate);
	}

	private static final class FieldTypeDate extends FieldType<LocalDate> {
		private final LocalDate startDate;

		FieldTypeDate() {
			this(LocalDate.parse("1970-01-01"));
		}

		FieldTypeDate(LocalDate startDate) {
			super(int.class, LocalDate.class, new SerializerDefInt(false, true), LOCAL_DATE_CODEC, INT_CODEC);
			this.startDate = startDate;
		}

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

	}

	public static final StructuredCodec<LocalDate> LOCAL_DATE_CODEC = new StructuredCodec<LocalDate>() {
		@Override
		public void encode(StructuredOutput out, LocalDate value) {
			out.writeString(value.toString());
		}

		@Override
		public LocalDate decode(StructuredInput in) throws MalformedDataException {
			try {
				return LocalDate.parse(in.readString());
			} catch (DateTimeException e) {
				throw new MalformedDataException(e);
			}
		}
	};

}
