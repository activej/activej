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

package io.activej.codegen.expression;

import io.activej.codegen.Context;
import org.jetbrains.annotations.NotNull;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static org.objectweb.asm.Type.getType;

final class ExpressionArraySet implements Expression {
	private final Expression array;
	private final Expression position;
	private final Expression newElement;

	ExpressionArraySet(@NotNull Expression array, @NotNull Expression position, @NotNull Expression newElement) {
		this.array = array;
		this.position = position;
		this.newElement = newElement;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Type arrayType = array.load(ctx);
		position.load(ctx);
		newElement.load(ctx);
		g.arrayStore(getType(arrayType.getDescriptor().substring(1)));
		return Type.VOID_TYPE;
	}
}
