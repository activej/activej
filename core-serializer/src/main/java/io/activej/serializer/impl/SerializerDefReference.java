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

package io.activej.serializer.impl;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.impl.SerializerExpressions.*;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;

public final class SerializerDefReference implements SerializerDef {
	public static final ThreadLocal<IdentityHashMap<Object, Integer>> MAP_ENCODE = ThreadLocal.withInitial(IdentityHashMap::new);
	public static final ThreadLocal<HashMap<Integer, Object>> MAP_DECODE = ThreadLocal.withInitial(HashMap::new);

	private final SerializerDef serializer;

	public SerializerDefReference(@NotNull SerializerDef serializer) {
		this.serializer = serializer;
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visit(serializer);
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
	}

	@Override
	public Class<?> getEncodeType() {
		return serializer.getEncodeType();
	}

	@Override
	public Map<Object, Expression> getEncoderInitializer() {
		return singletonMap(SerializerDefReference.class,
				call(cast(call(staticField(SerializerDefReference.class, "MAP_ENCODE"), "get"), IdentityHashMap.class), "clear"));
	}

	@Override
	public Map<Object, Expression> getDecoderInitializer() {
		return singletonMap(SerializerDefReference.class,
				call(cast(call(staticField(SerializerDefReference.class, "MAP_DECODE"), "get"), HashMap.class), "clear"));
	}

	@Override
	public final Expression defineEncoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return staticEncoders.define(this, getEncodeType(), buf, pos, value, version, compatibilityLevel);
	}

	@Override
	public final Expression defineDecoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return staticDecoders.define(this, getDecodeType(), in, version, compatibilityLevel);
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return let(cast(call(staticField(SerializerDefReference.class, "MAP_ENCODE"), "get"), IdentityHashMap.class),
				map -> let(call(map, "get", value),
						index -> ifThenElse(isNull(index),
								sequence(
										call(map, "put", value, cast(add(call(map, "size"), value(1)), Integer.class)),
										writeByte(buf, pos, value((byte) 0)),
										serializer.defineEncoder(staticEncoders, buf, pos, value, version, compatibilityLevel)
								),
								writeVarInt(buf, pos, cast(index, Integer.class)))));
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(cast(call(staticField(SerializerDefReference.class, "MAP_DECODE"), "get"), HashMap.class),
				map -> let(readVarInt(in),
						index -> {
							Function<Expression, Expression> extraInitializer = new Function<Expression, Expression>() {
								boolean initialized;
								@Override
								public Expression apply(Expression value) {
									if (initialized) return sequence();
									initialized = true;
									return call(map, "put", cast(add(call(map, "size"), value(1)), Integer.class), value);
								}
							};
							return ifThenElse(cmpEq(index, value(0)),
									let(serializer.decoderEx(staticDecoders, in, version, compatibilityLevel, extraInitializer),
											value -> sequence(
													extraInitializer.apply(value),
													value)),
									cast(call(map, "get", cast(index, Integer.class)), serializer.getDecodeType()));
						}));
	}
}
