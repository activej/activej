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

import io.activej.aggregation.util.JsonCodec;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.serializer.SerializerDef;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;

/**
 * Represents a type of aggregation field.
 */
public class FieldType<T> {
	private final Class<?> internalDataType;
	private final Type dataType;
	private final SerializerDef serializer;
	@Nullable
	private final JsonCodec<?> internalCodec;
	private final JsonCodec<T> codec;

	protected FieldType(Class<T> dataType, SerializerDef serializer, JsonCodec<T> codec) {
		this(dataType, dataType, serializer, codec, codec);
	}

	protected FieldType(Class<?> internalDataType, Type dataType, SerializerDef serializer, JsonCodec<T> codec, @Nullable JsonCodec<?> internalCodec) {
		this.internalDataType = internalDataType;
		this.dataType = dataType;
		this.serializer = serializer;
		this.internalCodec = internalCodec;
		this.codec = codec;
	}

	public final Class<?> getInternalDataType() {
		return internalDataType;
	}

	public final Type getDataType() {
		return dataType;
	}

	public SerializerDef getSerializer() {
		return serializer;
	}

	public Expression toValue(Expression internalValue) {
		return internalValue;
	}

	@Nullable
	public JsonCodec<?> getInternalCodec() {
		return internalCodec;
	}

	public JsonCodec<T> getCodec() {
		return codec;
	}

	@SuppressWarnings("unchecked")
	public T toInitialValue(Object internalValue) {
		return (T) internalValue;
	}

	public Object toInternalValue(T value) {
		return value;
	}

	public Expression toStringValue(Expression value) {
		return Expressions.staticCall(String.class, "valueOf", value);
	}

	@Override
	public String toString() {
		return "{" + internalDataType + '}';
	}

}
