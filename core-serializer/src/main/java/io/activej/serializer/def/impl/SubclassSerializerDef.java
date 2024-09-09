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

package io.activej.serializer.def.impl;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.builder.AbstractBuilder;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.def.AbstractSerializerDef;
import io.activej.serializer.def.SerializerDef;
import io.activej.serializer.def.SerializerDefWithNullable;
import io.activej.serializer.def.SerializerDefs;

import java.lang.reflect.Modifier;
import java.util.*;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkArgument;
import static io.activej.serializer.CompatibilityLevel.LEVEL_3;
import static io.activej.serializer.def.SerializerExpressions.readByte;
import static io.activej.serializer.def.SerializerExpressions.writeByte;
import static java.lang.String.format;

@ExposedInternals
public final class SubclassSerializerDef extends AbstractSerializerDef implements SerializerDefWithNullable {
	private static final int LOOKUP_SWITCH_THRESHOLD = 10;

	public final Class<?> parentType;
	public final LinkedHashMap<Class<?>, SerializerDef> subclassSerializers;
	public final boolean nullable;

	public int startIndex;

	public SubclassSerializerDef(Class<?> parentType, LinkedHashMap<Class<?>, SerializerDef> subclassSerializers, boolean nullable, int startIndex) {
		this.startIndex = startIndex;
		this.parentType = parentType;
		this.subclassSerializers = new LinkedHashMap<>(subclassSerializers);
		this.nullable = nullable;
	}

	public static Builder builder(Class<?> parentType) {
		return new SubclassSerializerDef(parentType, new LinkedHashMap<>(), false, 0).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, SubclassSerializerDef> {
		private Builder() {}

		public Builder withSubclass(Class<?> subclass, SerializerDef serializer) {
			checkNotBuilt(this);
			checkArgument(!Modifier.isAbstract(subclass.getModifiers()),
				"A subclass should not be an " +
				(subclass.isInterface() ? "interface" : "abstract class") + ": " + subclass);
			checkArgument(parentType.isAssignableFrom(subclass), format("Class %s should be a " +
																		(parentType.isInterface() ? "superinterface" : "superclass") +
																		" of the %s", parentType, subclass));
			SubclassSerializerDef.this.subclassSerializers.put(subclass, serializer);
			return this;
		}

		public Builder withStartIndex(int startIndex) {
			checkNotBuilt(this);
			SubclassSerializerDef.this.startIndex = startIndex;
			return this;
		}

		@Override
		protected SubclassSerializerDef doBuild() {
			return SubclassSerializerDef.this;
		}
	}

	@Override
	public SerializerDef ensureNullable(CompatibilityLevel compatibilityLevel) {
		if (compatibilityLevel.getLevel() < LEVEL_3.getLevel()) {
			return SerializerDefs.ofNullable(this);
		}
		return new SubclassSerializerDef(parentType, subclassSerializers, true, startIndex);
	}

	@Override
	public void accept(Visitor visitor) {
		for (Map.Entry<Class<?>, SerializerDef> entry : subclassSerializers.entrySet()) {
			visitor.visit(entry.getKey().getName(), entry.getValue());
		}
	}

	@Override
	public boolean isInline(int version, CompatibilityLevel compatibilityLevel) {
		return false;
	}

	@Override
	public Class<?> getEncodeType() {
		return parentType;
	}

	@Override
	public Expression encode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		int subClassIndex = (nullable && startIndex == 0 ? 1 : startIndex);

		List<Class<?>> subclasses = new ArrayList<>();
		List<Expression> subclassWriters = new ArrayList<>();
		for (Map.Entry<Class<?>, SerializerDef> entry : subclassSerializers.entrySet()) {
			SerializerDef subclassSerializer = entry.getValue();
			subclasses.add(entry.getKey());
			Encoder encoder = subclassSerializer.defineEncoder(staticEncoders, version, compatibilityLevel);
			subclassWriters.add(sequence(
				writeByte(buf, pos, value((byte) subClassIndex)),
				encoder.encode(buf, pos, cast(value, subclassSerializer.getEncodeType()))
			));

			subClassIndex++;
			if (nullable && subClassIndex == 0) {
				subClassIndex++;
			}
		}

		Expression writer = let(call(value, "getClass"), clazz -> {
			Expression unexpected = throwException(IllegalArgumentException.class, value("Unexpected subclass"));

			if (subclasses.size() >= LOOKUP_SWITCH_THRESHOLD) {
				Map<Integer, Expression> cases = new HashMap<>();
				for (int i = 0; i < subclasses.size(); i++) {
					cases.put(System.identityHashCode(subclasses.get(i)), subclassWriters.get(i));
				}
				if (cases.size() == subclasses.size()) {
					return tableSwitch(staticCall(System.class, "identityHashCode", clazz), cases, unexpected);
				}
			}

			Expression result = unexpected;
			for (int i = subclasses.size() - 1; i >= 0; i--) {
				result = ifRefNe(clazz, value(subclasses.get(i)), result, subclassWriters.get(i));
			}
			return result;
		});

		if (nullable) {
			return ifNonNull(value, writer, writeByte(buf, pos, value((byte) 0)));
		} else {
			return writer;
		}
	}

	@Override
	public Expression decode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(startIndex != 0 ? sub(readByte(in), value(startIndex)) : cast(readByte(in), int.class),
			idx -> {
				List<Expression> subclasses = new ArrayList<>();
				for (SerializerDef subclassSerializer : subclassSerializers.values()) {
					subclasses.add(cast(subclassSerializer.defineDecoder(staticDecoders, version, compatibilityLevel).decode(in), parentType));
				}
				if (nullable) subclasses.add(-startIndex, nullRef(getDecodeType()));
				Map<Integer, Expression> cases = new HashMap<>();
				for (int i = 0; i < subclasses.size(); i++) {
					cases.put(i, subclasses.get(i));
				}
				return cast(
					tableSwitch(idx, cases,
						throwException(CorruptedDataException.class, value("Unsupported subclass"))),
					parentType);
			});
	}
}
