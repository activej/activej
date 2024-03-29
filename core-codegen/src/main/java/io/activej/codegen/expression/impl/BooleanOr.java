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
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;

import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.is;
import static org.objectweb.asm.Type.BOOLEAN_TYPE;

/**
 * Defines methods for using logical 'or' for boolean type
 */
@ExposedInternals
public final class BooleanOr implements Expression {
	public final List<Expression> expressions;

	public BooleanOr(List<Expression> expressions) {
		this.expressions = expressions;
	}

	public List<Expression> getExpressions() {
		return expressions;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label exit = new Label();
		Label labelTrue = new Label();
		for (Expression predicate : expressions) {
			Type type = predicate.load(ctx);
			checkType(type, is(BOOLEAN_TYPE));
			g.ifZCmp(GeneratorAdapter.NE, labelTrue);
		}
		g.push(false);
		g.goTo(exit);

		g.mark(labelTrue);
		g.push(true);

		g.mark(exit);
		return BOOLEAN_TYPE;
	}
}
