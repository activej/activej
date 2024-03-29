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
import io.activej.common.annotation.ExposedInternals;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static io.activej.codegen.util.TypeChecks.*;
import static org.objectweb.asm.Type.getType;

@ExposedInternals
public final class ArraySet implements Expression {
	public final Expression array;
	public final Expression position;
	public final Expression newElement;

	public ArraySet(Expression array, Expression position, Expression newElement) {
		this.array = array;
		this.position = position;
		this.newElement = newElement;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Type arrayType = array.load(ctx);
		checkType(arrayType, isArray());

		Type positionType = position.load(ctx);
		checkType(positionType, isWidenedToInt());

		Type newElementType = newElement.load(ctx);
		checkType(newElementType, isAssignable());

		g.arrayStore(getType(arrayType.getDescriptor().substring(1)));
		return Type.VOID_TYPE;
	}
}
