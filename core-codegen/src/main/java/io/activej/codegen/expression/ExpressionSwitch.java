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
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;

import static io.activej.codegen.expression.Expressions.throwException;
import static io.activej.codegen.util.Utils.isPrimitiveType;
import static org.objectweb.asm.Type.getType;

final class ExpressionSwitch implements Expression {
	public static final Expression DEFAULT_EXPRESSION = throwException(IllegalArgumentException.class);

	private final Expression value;
	private final List<Expression> matchCases;
	private final List<Expression> matchExpressions;
	private final Expression defaultExpression;

	ExpressionSwitch(Expression value, List<Expression> matchCases, List<Expression> matchExpressions, Expression defaultExpression) {
		this.value = value;
		this.matchCases = matchCases;
		this.matchExpressions = matchExpressions;
		this.defaultExpression = defaultExpression;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Type keyType = this.value.load(ctx);
		VarLocal value = ctx.newLocal(keyType);
		value.store(ctx);

		Label labelExit = new Label();

		Type resultType = getType(Object.class);
		for (int i = 0; i < matchCases.size(); i++) {
			Label labelNext = new Label();
			if (isPrimitiveType(keyType) || keyType.equals(getType(Class.class))) {
				matchCases.get(i).load(ctx);
				value.load(ctx);
				g.ifCmp(keyType, GeneratorAdapter.NE, labelNext);
			} else {
				ctx.invoke(matchCases.get(i), "equals", value);
				g.push(true);
				g.ifCmp(Type.BOOLEAN_TYPE, GeneratorAdapter.NE, labelNext);
			}

			resultType = matchExpressions.get(i).load(ctx);
			g.goTo(labelExit);

			g.mark(labelNext);
		}

		defaultExpression.load(ctx);

		g.mark(labelExit);

		return resultType;
	}
}
