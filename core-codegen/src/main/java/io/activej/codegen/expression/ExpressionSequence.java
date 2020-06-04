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
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;

import static org.objectweb.asm.Type.VOID_TYPE;

/**
 * Defines methods which allow to use several methods one after the other
 */
final class ExpressionSequence implements Expression {
	final List<Expression> expressions;

	ExpressionSequence(List<Expression> expressions) {
		this.expressions = expressions;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Type type = VOID_TYPE;
		for (int i = 0; i < expressions.size(); i++) {
			Expression expression = expressions.get(i);
			type = expression.load(ctx);
			if (i != expressions.size() - 1) {
				if (type.getSize() == 1)
					g.pop();
				if (type.getSize() == 2)
					g.pop2();
			}
		}

		return type;
	}
}
