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

package io.activej.codegen.util;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Type;

import java.util.Objects;
import java.util.function.Predicate;

import static io.activej.codegen.util.Utils.isEqualType;

public final class TypeChecks {

	@Contract("null, _ -> fail")
	public static void checkType(@Nullable Type type, Predicate<@Nullable Type> predicate) {
		if (!predicate.test(type)) {
			throw new IllegalArgumentException("Illegal type: " +
					(type == null ?
							"'throw' type" :
							type.getClassName()));
		}
		assert type != null;
	}

	@Contract("null, _, _ -> fail")
	public static void checkType(@Nullable Type type, Predicate<@Nullable Type> predicate, String message) {
		if (!predicate.test(type)) {
			throw new IllegalArgumentException(message);
		}
		assert type != null;
	}

	public static Predicate<@Nullable Type> isNotThrow() {
		return Objects::nonNull;
	}

	public static Predicate<@Nullable Type> is(Type type, Type... otherTypes) {
		return testedType -> {
			if (testedType == null) return false;

			if (isEqualType(testedType, type)) return true;

			for (Type otherType : otherTypes) {
				if (isEqualType(testedType, otherType)) {
					return true;
				}
			}
			return false;
		};
	}

	public static Predicate<@Nullable Type> isWidenedToInt() {
		return type -> type != null && type.getSort() >= Type.CHAR && type.getSort() <= Type.INT;
	}

	public static Predicate<@Nullable Type> isPrimitive() {
		return type -> type != null && Utils.isPrimitiveType(type);
	}

	public static Predicate<@Nullable Type> isWrapper() {
		return type -> type != null && Utils.isWrapperType(type);
	}

	public static Predicate<@Nullable Type> isObject() {
		return type -> type != null && type.getSort() == Type.OBJECT;
	}

	public static Predicate<@Nullable Type> isArray() {
		return type -> type != null && type.getSort() == Type.ARRAY;
	}

	public static Predicate<@Nullable Type> isAssignable() {
		return type -> type != null && type.getSort() > Type.VOID && type.getSort() <= Type.OBJECT;
	}

	public static Predicate<@Nullable Type> isArithmetic() {
		return type -> type != null && type.getSort() >= Type.CHAR && type.getSort() <= Type.DOUBLE;
	}
}
