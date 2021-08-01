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
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class RecursiveType {
	private static final RecursiveType[] NO_TYPE_PARAMS = new RecursiveType[0];

	@NotNull
	private final Class<?> clazz;
	@NotNull
	private final RecursiveType[] typeArguments;

	private RecursiveType(@NotNull Class<?> clazz, @NotNull RecursiveType[] typeArguments) {
		this.clazz = clazz;
		this.typeArguments = typeArguments;
	}

	@NotNull
	public static RecursiveType of(@NotNull Class<?> clazz) {
		return of(clazz, NO_TYPE_PARAMS);
	}

	@NotNull
	public static RecursiveType of(@NotNull Class<?> clazz, @NotNull List<RecursiveType> typeParams) {
		return of(clazz, typeParams.toArray(new RecursiveType[0]));
	}

	@NotNull
	public static RecursiveType of(@NotNull Class<?> clazz, @NotNull RecursiveType... typeParams) {
		if (clazz.isArray()) throw new IllegalArgumentException("Unsupported type: " + clazz);
		return new RecursiveType(clazz, typeParams);
	}

	@NotNull
	public static RecursiveType of(@NotNull TypeT<?> type) {
		return of(type.getType());
	}

	@NotNull
	public static RecursiveType of(@NotNull Type type) {
		if (type instanceof Class) {
			return of((Class<?>) type);
		} else if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			return of((Class<?>) parameterizedType.getRawType(),
					Arrays.stream(parameterizedType.getActualTypeArguments())
							.map(RecursiveType::of)
							.toArray(RecursiveType[]::new));
		} else if (type instanceof WildcardType) {
			return of(TypeUtils.getRawType(type));
		} else {
			throw new IllegalArgumentException(type.getTypeName());
		}
	}

	@NotNull
	public Class<?> getRawType() {
		return clazz;
	}

	public RecursiveType[] getTypeArguments() {
		return typeArguments;
	}

	@NotNull
	public Type getType() {
		return typeArguments.length == 0 ? clazz : new ParameterizedType() {
			final Type[] actualTypeArguments = Arrays.stream(typeArguments).map(RecursiveType::getType).toArray(Type[]::new);

			@Override
			public Type[] getActualTypeArguments() {
				return actualTypeArguments;
			}

			@Override
			public Type getRawType() {
				return clazz;
			}

			@Override
			public Type getOwnerType() {
				return null;
			}

			@Override
			public String toString() {
				return RecursiveType.this.toString();
			}
		};
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RecursiveType that = (RecursiveType) o;
		if (!clazz.equals(that.clazz)) return false;
		if (!Arrays.equals(typeArguments, that.typeArguments)) return false;
		return true;
	}

	@Override
	public int hashCode() {
		int result = clazz.hashCode();
		result = 31 * result + Arrays.hashCode(typeArguments);
		return result;
	}

	@NotNull
	public String getSimpleName() {
		return clazz.getSimpleName() + (typeArguments.length == 0 ? "" :
				Arrays.stream(typeArguments)
						.map(RecursiveType::getSimpleName)
						.collect(Collectors.joining(",", "<", ">")));
	}

	@NotNull
	public String getName() {
		return clazz.getName() + (typeArguments.length == 0 ? "" :
				Arrays.stream(typeArguments)
						.map(RecursiveType::getName)
						.collect(Collectors.joining(",", "<", ">")));
	}

	@Nullable
	public String getPackage() {
		Package pkg = clazz.getPackage();
		return pkg != null ? pkg.getName() : null;
	}

	@NotNull
	@Override
	public String toString() {
		return getName();
	}
}
