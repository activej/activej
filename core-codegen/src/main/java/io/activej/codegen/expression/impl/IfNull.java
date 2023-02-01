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
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isAssignable;
import static io.activej.codegen.util.Utils.isPrimitiveType;

@ExplicitlyExposed
public final class IfNull implements Expression {
	private final Expression expression;
	private final Expression expressionTrue;
	private final Expression expressionFalse;

	public IfNull(Expression expression, Expression expressionTrue, Expression expressionFalse) {
		this.expression = expression;
		this.expressionTrue = expressionTrue;
		this.expressionFalse = expressionFalse;
	}

	public Expression getExpression() {
		return expression;
	}

	public Expression getExpressionTrue() {
		return expressionTrue;
	}

	public Expression getExpressionFalse() {
		return expressionFalse;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Label labelTrue = new Label();
		Label labelExit = new Label();

		Type argType = expression.load(ctx);
		checkType(argType, isAssignable());

		if (isPrimitiveType(argType)) {
			if (argType.getSize() == 1)
				g.pop();
			if (argType.getSize() == 2)
				g.pop2();
			return expressionFalse.load(ctx);
		}

		g.ifNull(labelTrue);

		Type typeFalse = expressionFalse.load(ctx);
		g.goTo(labelExit);

		g.mark(labelTrue);
		Type typeTrue = expressionTrue.load(ctx);

		g.mark(labelExit);

		return ctx.unifyTypes(typeFalse, typeTrue);
	}
}
