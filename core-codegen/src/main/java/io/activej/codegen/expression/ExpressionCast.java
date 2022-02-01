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

import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isAssignable;
import static io.activej.codegen.util.Utils.isPrimitiveType;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.InstructionAdapter.OBJECT_TYPE;

/**
 * Defines method in order to cast a function to a type
 */
final class ExpressionCast implements Expression {
	static final Type SELF_TYPE = getType(Object.class);

	private final Expression expression;
	private final Type targetType;

	ExpressionCast(@NotNull Expression expression, @NotNull Type type) {
		this.expression = expression;
		this.targetType = type;
	}

	@Override
	public Type load(Context ctx) {
		Type targetType = this.targetType == SELF_TYPE ? ctx.getSelfType() : this.targetType;

		Type sourceType = expression.load(ctx);
		checkType(sourceType, isAssignable());
		if (!targetType.equals(OBJECT_TYPE) || isPrimitiveType(sourceType)) {
			ctx.cast(sourceType, targetType);
		}
		return targetType;
	}
}
