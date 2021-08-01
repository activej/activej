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

package io.activej.inject;

import io.activej.inject.util.ReflectionUtils;
import io.activej.inject.util.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;
import java.util.function.Predicate;

import static io.activej.types.IsAssignableUtils.isAssignable;

public abstract class KeyPattern<T> {
	@NotNull
	private final Type type;
	private final Predicate<?> qualifier;

	public KeyPattern() {
		this.type = getTypeParameter();
		this.qualifier = null;
	}

	public KeyPattern(Object qualifier) {
		this.type = getTypeParameter();
		this.qualifier = predicateOf(qualifier);
	}

	public KeyPattern(Predicate<?> qualifier) {
		this.type = getTypeParameter();
		this.qualifier = qualifier;
	}

	KeyPattern(@NotNull Type type, Predicate<?> qualifier) {
		this.type = type;
		this.qualifier = qualifier;
	}

	/**
	 * A default subclass to be used by {@link #of KeyPattern.of*} and {@link #ofType KeyPattern.ofType*} constructors
	 */
	private static final class KeyImpl<T> extends KeyPattern<T> {
		private KeyImpl(Type type, Predicate<?> qualifierPredicate) {
			super(type, qualifierPredicate);
		}
	}

	@NotNull
	public static <T> KeyPattern<T> of(@NotNull Class<T> type) {
		return new KeyImpl<>(type, null);
	}

	@NotNull
	public static <T> KeyPattern<T> of(@NotNull Class<T> type, Object qualifier) {
		return new KeyImpl<>(type, predicateOf(qualifier));
	}

	@NotNull
	public static <T> KeyPattern<T> of(@NotNull Class<T> type, Predicate<?> qualifier) {
		return new KeyImpl<>(type, qualifier);
	}

	@NotNull
	public static <T> KeyPattern<T> ofType(@NotNull Type type) {
		return new KeyImpl<>(type, null);
	}

	@NotNull
	public static <T> KeyPattern<T> ofType(@NotNull Type type, Object qualifier) {
		return new KeyImpl<>(type, predicateOf(qualifier));
	}

	@NotNull
	public static <T> KeyPattern<T> ofType(@NotNull Type type, Predicate<?> qualifier) {
		return new KeyImpl<>(type, qualifier);
	}

	@NotNull
	private static Predicate<Object> predicateOf(Object qualifier) {
		return q -> Objects.equals(q, qualifier);
	}

	/**
	 * Returns a new key with same type but the qualifier replaced with a given one
	 */
	public KeyPattern<T> qualified(Object qualifier) {
		return new KeyImpl<>(type, predicateOf(qualifier));
	}

	/**
	 * Returns a new key with same type but the qualifier replaced with a given one
	 */
	public KeyPattern<T> qualified(Predicate<?> qualifier) {
		return new KeyImpl<>(type, qualifier);
	}

	public @NotNull Type getType() {
		return type;
	}

	public Predicate<?> getQualifier() {
		return qualifier;
	}

	public boolean hasQualifier() {
		return qualifier != null;
	}

	public boolean match(Key<?> key) {
		//noinspection unchecked
		return isAssignable(this.type, key.getType()) &&
				(this.qualifier == null || ((Predicate<Object>) this.qualifier).test(key.getQualifier()));
	}

	@NotNull
	private Type getTypeParameter() {
		// this cannot possibly fail so not even a check here
		Type typeArgument = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
		Object outerInstance = ReflectionUtils.getOuterClassInstance(this);
		// the outer instance is null in static context
		return outerInstance != null ? TypeUtils.resolveTypeVariables(typeArgument, outerInstance.getClass(), outerInstance) : typeArgument;
	}

	@Override
	public String toString() {
		return (qualifier != null ? qualifier + " " : "") + type.getTypeName();
	}
}
