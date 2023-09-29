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

package io.activej.cube.aggregation.fieldtype;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.json.JsonCodec;
import io.activej.serializer.def.SerializerDef;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;

/**
 * Represents a type of aggregation field.
 */
public class FieldType<T> {
	private final Class<?> internalDataType;
	private final Type dataType;
	private final SerializerDef serializer;
	private final @Nullable JsonCodec<?> internalJsonCodec;
	private final JsonCodec<T> jsonCodec;

	protected FieldType(Class<T> dataType, SerializerDef serializer, JsonCodec<T> jsonCodec) {
		this(dataType, dataType, serializer, jsonCodec, jsonCodec);
	}

	protected FieldType(Class<?> internalDataType, Type dataType, SerializerDef serializer, JsonCodec<T> jsonCodec, @Nullable JsonCodec<?> internalJsonCodec) {
		this.internalDataType = internalDataType;
		this.dataType = dataType;
		this.serializer = serializer;
		this.internalJsonCodec = internalJsonCodec;
		this.jsonCodec = jsonCodec;
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

	public @Nullable JsonCodec<?> getInternalJsonCodec() {
		return internalJsonCodec;
	}

	public JsonCodec<T> getJsonCodec() {
		return jsonCodec;
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
