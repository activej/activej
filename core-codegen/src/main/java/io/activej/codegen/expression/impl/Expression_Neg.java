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
import static io.activej.codegen.util.TypeChecks.isPrimitive;
import static org.objectweb.asm.Type.BOOLEAN_TYPE;
import static org.objectweb.asm.Type.INT_TYPE;

@ExplicitlyExposed
public final class Expression_Neg implements Expression {
	private final Expression arg;

	public Expression_Neg(Expression arg) {
		this.arg = arg;
	}

	public Expression getArg() {
		return arg;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Type argType = arg.load(ctx);
		checkType(argType, isPrimitive());

		int argSort = argType.getSort();

		if (argSort == Type.DOUBLE || argSort == Type.FLOAT || argSort == Type.LONG || argSort == Type.INT) {
			g.math(GeneratorAdapter.NEG, argType);
			return argType;
		}
		if (argSort == Type.BYTE || argSort == Type.SHORT || argSort == Type.CHAR) {
//			g.cast(argType, INT_TYPE);
			g.math(GeneratorAdapter.NEG, INT_TYPE);
			return argType;
		}

		assert argSort == Type.BOOLEAN;

		Label labelTrue = new Label();
		Label labelExit = new Label();
		g.push(true);
		g.ifCmp(BOOLEAN_TYPE, GeneratorAdapter.EQ, labelTrue);
		g.push(true);
		g.goTo(labelExit);

		g.mark(labelTrue);
		g.push(false);

		g.mark(labelExit);
		return INT_TYPE;
	}
}
