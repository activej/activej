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
import io.activej.codegen.Expression;
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
	private final StructuredCodec<?> internalCodec;
	private final StructuredCodec<T> codec;

	protected FieldType(Class<T> dataType, SerializerDef serializer, StructuredCodec<T> codec) {
		this(dataType, dataType, serializer, codec, codec);
	}

	protected FieldType(Class<?> internalDataType, Type dataType, SerializerDef serializer, StructuredCodec<T> codec, @Nullable StructuredCodec<?> internalCodec) {
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

	public StructuredCodec<T> getCodec() {
		return codec;
	}

	@Nullable
	public StructuredCodec<?> getInternalCodec() {
		return internalCodec;
	}

	public Expression toValue(Expression internalValue) {
		return internalValue;
	}

	public Object toInternalValue(T value) {
		return value;
	}

	@Override
	public String toString() {
		return "{" + internalDataType + '}';
	}

}
