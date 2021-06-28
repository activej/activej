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

package io.activej.common.reflection;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkArgument;
import static java.util.stream.Collectors.toList;

public final class RecursiveType {
	private static final RecursiveType[] NO_TYPE_PARAMS = new RecursiveType[0];

	@NotNull
	private final Class<?> clazz;
	@NotNull
	private final RecursiveType[] typeParams;

	private final int arrayDimension;

	private RecursiveType(@NotNull Class<?> clazz, @NotNull RecursiveType[] typeParams, int arrayDimension) {
		this.clazz = clazz;
		this.typeParams = typeParams;
		this.arrayDimension = arrayDimension;
	}

	@NotNull
	public static RecursiveType of(@NotNull Class<?> clazz) {
		return new RecursiveType(clazz, NO_TYPE_PARAMS, clazz.isArray() ? 1 : 0);
	}

	@NotNull
	public static RecursiveType of(@NotNull Class<?> clazz, @NotNull RecursiveType... typeParams) {
		return new RecursiveType(clazz, typeParams, clazz.isArray() ? 1 : 0);
	}

	@NotNull
	public static RecursiveType of(@NotNull Class<?> clazz, @NotNull List<RecursiveType> typeParams) {
		return new RecursiveType(clazz, typeParams.toArray(new RecursiveType[0]), clazz.isArray() ? 1 : 0);
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
			Type[] upperBounds = ((WildcardType) type).getUpperBounds();
			checkArgument(upperBounds.length == 1, type);
			return of(upperBounds[0]);
		} else if (type instanceof GenericArrayType) {
			RecursiveType component = of(((GenericArrayType) type).getGenericComponentType());
			return new RecursiveType(component.clazz, component.typeParams, component.arrayDimension + 1);
		} else {
			throw new IllegalArgumentException(type.getTypeName());
		}
	}

	@NotNull
	public Class<?> getRawType() {
		return clazz;
	}

	@NotNull
	public RecursiveType[] getTypeParams() {
		return typeParams;
	}

	public boolean isArray() {
		return arrayDimension != 0;
	}

	public int getArrayDimension() {
		return arrayDimension;
	}

	@NotNull
	private Type getArrayType(@NotNull Type component, int arrayDeepness) {
		if (arrayDeepness == 0) {
			return component;
		}
		return (GenericArrayType) () -> getArrayType(component, arrayDeepness - 1);
	}

	@NotNull
	public Type getType() {
		if (typeParams.length == 0) {
			return getArrayType(clazz, arrayDimension);
		}

		Type[] types = Arrays.stream(typeParams)
				.map(RecursiveType::getType)
				.collect(toList())
				.toArray(new Type[]{});

		ParameterizedType parameterized = getParameterized(clazz, types);
		return getArrayType(parameterized, arrayDimension);
	}

	@NotNull
	private static ParameterizedType getParameterized(Class<?> clazz, Type[] types) {
		return new ParameterizedType() {
			@NotNull
			@Override
			public Type[] getActualTypeArguments() {
				return types;
			}

			@NotNull
			@Override
			public Type getRawType() {
				return clazz;
			}

			@Nullable
			@Override
			public Type getOwnerType() {
				return null;
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
		if (!Arrays.equals(typeParams, that.typeParams)) return false;
		return true;
	}

	@Override
	public int hashCode() {
		int result = clazz.hashCode();
		result = 31 * result + Arrays.hashCode(typeParams);
		return result;
	}

	@NotNull
	public String getSimpleName() {
		return clazz.getSimpleName() + (typeParams.length == 0 ? "" :
				Arrays.stream(typeParams)
						.map(RecursiveType::getSimpleName)
						.collect(Collectors.joining(",", "<", ">"))) + new String(new char[arrayDimension]).replace("\0", "[]");
	}

	@NotNull
	public String getName() {
		return clazz.getName() + (typeParams.length == 0 ? "" :
				Arrays.stream(typeParams)
						.map(RecursiveType::getName)
						.collect(Collectors.joining(",", "<", ">"))) + new String(new char[arrayDimension]).replace("\0", "[]");
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
