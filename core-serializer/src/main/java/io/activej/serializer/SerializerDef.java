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

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;

import java.util.Set;

import static io.activej.codegen.expression.Expressions.arg;

/**
 * Represents a serializer and deserializer of a particular class to byte arrays
 */
public interface SerializerDef {

	interface Visitor {
		void visit(String serializerId, SerializerDef serializer);

		default void visit(SerializerDef serializer) {
			visit("", serializer);
		}
	}

	void accept(Visitor visitor);

	Set<Integer> getVersions();

	/**
	 * Returns the raw type of object which will be serialized
	 *
	 * @return type of object which will be serialized
	 */
	Class<?> getEncodeType();

	Class<?> getDecodeType();

	boolean isInline(int version, CompatibilityLevel compatibilityLevel);

	Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel);

	Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel);

	interface StaticEncoders {
		Expression BUF = arg(0);
		Variable POS = arg(1);
		Variable VALUE = arg(2);

		Expression define(SerializerDef serializerDef, Expression buf, Variable pos, Expression value);
	}

	interface StaticDecoders {
		Variable IN = arg(0);

		Expression define(SerializerDef serializerDef, Expression in);

		<T> Class<T> buildClass(ClassBuilder<T> classBuilder);
	}

	/**
	 * Serializes provided {@link Expression} {@code value} to byte array
	 *
	 * @param buf                byte array to which the value will be serialized
	 * @param pos                an offset in the byte array
	 * @param value              the value to be serialized to byte array
	 * @param compatibilityLevel defines the {@link CompatibilityLevel compatibility level} of the serializer
	 * @return serialized to byte array value
	 */
	 default Expression defineEncoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		 return isInline(version, compatibilityLevel) ?
				 encoder(staticEncoders, buf, pos, value, version, compatibilityLevel) :
				 staticEncoders.define(this, buf, pos, value);
	 }

	/**
	 * Deserializes object from byte array
	 *
	 * @param in                 BinaryInput
	 * @param compatibilityLevel defines the {@link CompatibilityLevel compatibility level} of the serializer
	 * @return deserialized {@code Expression} object of provided targetType
	 */
	default Expression defineDecoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return isInline(version, compatibilityLevel) ?
				decoder(staticDecoders, in, version, compatibilityLevel) :
				staticDecoders.define(this, in);
	}
}
