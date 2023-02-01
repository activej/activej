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
import static io.activej.codegen.util.TypeChecks.is;
import static org.objectweb.asm.Type.BOOLEAN_TYPE;

@ExplicitlyExposed
public final class LoopExpression implements Expression {
	private final Expression condition;
	private final Expression body;

	public LoopExpression(Expression condition, Expression body) {
		this.condition = condition;
		this.body = body;
	}

	public Expression getCondition() {
		return condition;
	}

	public Expression getBody() {
		return body;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label labelLoop = new Label();
		Label labelExit = new Label();

		g.mark(labelLoop);

		Type conditionType = condition.load(ctx);
		checkType(conditionType, is(BOOLEAN_TYPE));

		g.push(false);
		g.ifCmp(BOOLEAN_TYPE, GeneratorAdapter.EQ, labelExit);

		Type bodyType = body.load(ctx);
		if (bodyType != null) {
			if (bodyType.getSize() == 1)
				g.pop();
			if (bodyType.getSize() == 2)
				g.pop2();
		}

		g.goTo(labelLoop);
		g.mark(labelExit);
		return Type.VOID_TYPE;
	}
}
