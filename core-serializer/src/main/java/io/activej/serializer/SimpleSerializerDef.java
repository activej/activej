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

package io.activej.serializer;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.types.Types;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.activej.codegen.expression.Expressions.*;

public abstract class SimpleSerializerDef<T> extends AbstractSerializerDef {
	private final Map<CacheKey, Expression> SERIALIZER_EXPRESSION_CACHE = new HashMap<>();

	private final Class<T> encodeType;

	protected SimpleSerializerDef() {
		//noinspection unchecked
		encodeType = (Class<T>) Types.getRawType(getSuperclassTypeParameter(getClass()));
	}

	protected abstract BinarySerializer<T> createSerializer(int version, CompatibilityLevel compatibilityLevel);

	public final Class<T> getEncodeType() {
		return encodeType;
	}

	@Override
	public final Expression encode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		Expression serializer = ensureSerializerExpression(version, compatibilityLevel);
		return set(pos, call(serializer, "encode", buf, pos, value));
	}

	@Override
	public final Expression decode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		Expression serializer = ensureSerializerExpression(version, compatibilityLevel);
		return call(serializer, "decode", in);
	}

	private static Type getSuperclassTypeParameter(Class<?> subclass) {
		Type superclass = subclass.getGenericSuperclass();
		if (superclass instanceof ParameterizedType) {
			return ((ParameterizedType) superclass).getActualTypeArguments()[0];
		}
		throw new AssertionError();
	}

	private synchronized Expression ensureSerializerExpression(int version, CompatibilityLevel compatibilityLevel) {
		CacheKey cacheKey = new CacheKey(version, compatibilityLevel);
		return SERIALIZER_EXPRESSION_CACHE.computeIfAbsent(cacheKey, $ -> value(createSerializer(version, compatibilityLevel), BinarySerializer.class));
	}

	public static final class CacheKey {
		private final int version;
		private final CompatibilityLevel level;

		private CacheKey(int version, CompatibilityLevel level) {
			this.version = version;
			this.level = level;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			CacheKey cacheKey = (CacheKey) o;
			return version == cacheKey.version && level == cacheKey.level;
		}

		@Override
		public int hashCode() {
			return Objects.hash(version, level);
		}
	}
}
