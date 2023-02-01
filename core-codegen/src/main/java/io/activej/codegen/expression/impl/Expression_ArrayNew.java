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

package io.activej.codegen.expression.impl;

import io.activej.codegen.Context;
import io.activej.codegen.expression.Expression;
import io.activej.common.annotation.ExplicitlyExposed;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isWidenedToInt;

@ExplicitlyExposed
public final class Expression_ArrayNew implements Expression {
	private final Class<?> type;
	private final Expression length;

	public Expression_ArrayNew(Class<?> type, Expression length) {
		if (!type.isArray())
			throw new IllegalArgumentException();
		this.type = type;
		this.length = length;
	}

	public Class<?> getType() {
		return type;
	}

	public Expression getLength() {
		return length;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Type lengthType = length.load(ctx);
		checkType(lengthType, isWidenedToInt());
		g.newArray(Type.getType(Type.getType(type).getDescriptor().substring(1)));
		return Type.getType(type);
	}
}
