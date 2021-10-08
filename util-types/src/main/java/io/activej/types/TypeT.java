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

package io.activej.types;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Type;

/**
 * A type token for defining complex types (annotated, parameterized)
 * <p>
 * Usage example:
 * <p>
 * {@code Type listOfStringsType = new TypeT<List<String>>(){}.getType()}
 * <p>
 * <b>Note that Java 8 does not seem to resolve annotations on annotated types correctly.
 * If you need to use {@link TypeT} with annotated type while on Java 8, you can use
 * {@link TypeT#ofAnnotatedType(AnnotatedType)} method instead of {@link TypeT} constructor</b>
 *
 * @param <T> actual type
 */
public abstract class TypeT<T> {
	private final @NotNull AnnotatedType annotatedType;

	/**
	 * Creates a new type token. A type argument {@link T} <b>must</b> be specified.
	 * A typical usage is:
	 * <p>
	 * {@code TypeT<List<Integer>> integerListTypeT = new TypeT<List<Integer>>(){};}
	 *
	 * @throws AssertionError if a {@link TypeT} is created with a raw type
	 */
	protected TypeT() {
		this.annotatedType = getSuperclassTypeParameter(this.getClass());
	}

	private TypeT(@NotNull AnnotatedType annotatedType) {
		this.annotatedType = annotatedType;
	}

	/**
	 * Constructs a new {@link TypeT} out of given {@link AnnotatedType}
	 */
	public static <T> @NotNull TypeT<T> ofAnnotatedType(@NotNull AnnotatedType annotatedType) {
		return new TypeT<T>(annotatedType) {};
	}

	private static @NotNull AnnotatedType getSuperclassTypeParameter(@NotNull Class<?> subclass) {
		AnnotatedType superclass = subclass.getAnnotatedSuperclass();
		if (superclass instanceof AnnotatedParameterizedType) {
			return ((AnnotatedParameterizedType) superclass).getAnnotatedActualTypeArguments()[0];
		}
		throw new AssertionError();
	}

	/**
	 * Returns an {@link AnnotatedType} of a {@link T}
	 */
	public final @NotNull AnnotatedType getAnnotatedType() {
		return annotatedType;
	}

	/**
	 * Returns a {@link Type} of a {@link T}
	 */
	public final @NotNull Type getType() {
		return annotatedType.getType();
	}

	/**
	 * Returns a raw type (e.g {@link Class}) of a {@link T}
	 */
	@SuppressWarnings("unchecked")
	public final Class<T> getRawType() {
		return (Class<T>) Types.getRawType(annotatedType.getType());
	}

	@Override
	public final String toString() {
		return annotatedType.toString();
	}

}
