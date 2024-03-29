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
import io.activej.types.Types;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;
import java.util.function.Predicate;

import static io.activej.inject.util.TypeUtils.simplifyType;
import static io.activej.types.IsAssignableUtils.isAssignable;

/**
 * A pattern to match a dependency injection {@link Key}
 * <p>
 * A {@link Key} is matched if a {@link Key#getType() key's type} is assignable to
 * this pattern's {@link #type} and this pattern's qualifier is {@code null} or matches
 * a {@link Key#getQualifier()}  key's qualifier
 */
@SuppressWarnings("unused") // <T> is required to obtain type from type parameter
public abstract class KeyPattern<T> {
	private final Type type;
	private final Predicate<?> qualifier;

	protected KeyPattern() {
		this.type = simplifyType(getTypeParameter());
		this.qualifier = null;
	}

	protected KeyPattern(Object qualifier) {
		this.type = simplifyType(getTypeParameter());
		this.qualifier = predicateOf(qualifier);
	}

	protected KeyPattern(Predicate<?> qualifier) {
		this.type = simplifyType(getTypeParameter());
		this.qualifier = qualifier;
	}

	KeyPattern(Type type, Predicate<?> qualifier) {
		this.type = simplifyType(type);
		this.qualifier = qualifier;
	}

	/**
	 * A default subclass to be used by {@link #of KeyPattern.of*} and {@link #ofType KeyPattern.ofType*} constructors
	 */
	public static final class KeyPatternImpl<T> extends KeyPattern<T> {
		private KeyPatternImpl(Type type, Predicate<?> qualifierPredicate) {
			super(type, qualifierPredicate);
		}
	}

	public static <T> KeyPattern<T> create(Type type, Predicate<?> qualifier) {
		return new KeyPatternImpl<>(type, qualifier);
	}

	public static <T> KeyPattern<T> of(Class<T> type) {
		return new KeyPatternImpl<>(type, null);
	}

	public static <T> KeyPattern<T> of(Class<T> type, Object qualifier) {
		return new KeyPatternImpl<>(type, predicateOf(qualifier));
	}

	public static <T> KeyPattern<T> ofType(Type type) {
		return new KeyPatternImpl<>(type, null);
	}

	public static <T> KeyPattern<T> ofType(Type type, Object qualifier) {
		return new KeyPatternImpl<>(type, predicateOf(qualifier));
	}

	private static Predicate<Object> predicateOf(Object qualifier) {
		return q -> Objects.equals(q, qualifier);
	}

	public Type getType() {
		return type;
	}

	public boolean hasQualifier() {
		return qualifier != null;
	}

	public boolean match(Key<?> key) {
		//noinspection unchecked
		return
			isAssignable(this.type, key.getType()) &&
			(this.qualifier == null || ((Predicate<Object>) this.qualifier).test(key.getQualifier()));
	}

	private Type getTypeParameter() {
		// this cannot possibly fail so not even a check here
		Type typeArgument = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
		Object outerInstance = ReflectionUtils.getOuterClassInstance(this);
		// the outer instance is null in static context
		return outerInstance != null ? Types.bind(typeArgument, Types.getAllTypeBindings(outerInstance.getClass())) : typeArgument;
	}

	@Override
	public String toString() {
		return (qualifier != null ? qualifier + " " : "") + type.getTypeName();
	}
}
