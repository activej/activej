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

import java.util.List;

import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isNotThrow;
import static org.objectweb.asm.Type.VOID_TYPE;

/**
 * Defines methods which allow to use several methods one after the other
 */
@ExplicitlyExposed
public final class Sequence implements Expression {
	private final List<Expression> expressions;

	public Sequence(List<Expression> expressions) {
		this.expressions = expressions;
	}

	public List<Expression> getExpressions() {
		return expressions;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Type type = VOID_TYPE;
		for (int i = 0; i < expressions.size(); i++) {
			Expression expression = expressions.get(i);
			type = expression.load(ctx);
			if (i != expressions.size() - 1) {
				checkType(type, isNotThrow(), "There are additional expressions in a sequence after a 'throw' exception");

				if (type.getSize() == 1)
					g.pop();
				if (type.getSize() == 2)
					g.pop2();
			}
		}

		return type;
	}
}
