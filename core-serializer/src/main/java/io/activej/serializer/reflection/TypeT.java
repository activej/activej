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

package io.activej.serializer.reflection;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class TypeT<T> {
	private final @NotNull AnnotatedType annotatedType;

	public TypeT() {
		this.annotatedType = getSuperclassTypeParameter(this.getClass());
	}

	@NotNull
	private static AnnotatedType getSuperclassTypeParameter(@NotNull Class<?> subclass) {
		AnnotatedType superclass = subclass.getAnnotatedSuperclass();
		if (superclass instanceof AnnotatedParameterizedType) {
			return ((AnnotatedParameterizedType) superclass).getAnnotatedActualTypeArguments()[0];
		}
		throw new AssertionError();
	}

	@NotNull
	public AnnotatedType getAnnotatedType() {
		return annotatedType;
	}

	@NotNull
	public Type getType() {
		return annotatedType.getType();
	}

	@SuppressWarnings("unchecked")
	public Class<T> getRawType() {
		Type type = annotatedType.getType();
		if (type instanceof Class) {
			return (Class<T>) type;
		} else if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			return (Class<T>) parameterizedType.getRawType();
		} else {
			throw new IllegalArgumentException(type.getTypeName());
		}
	}

	@Override
	public String toString() {
		return annotatedType.toString();
	}
}
