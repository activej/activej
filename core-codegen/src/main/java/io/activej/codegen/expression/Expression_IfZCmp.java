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
import io.activej.codegen.operation.CompareOperation;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static io.activej.codegen.util.TypeChecks.checkType;
import static org.objectweb.asm.Type.BOOLEAN;
import static org.objectweb.asm.Type.INT;

public final class Expression_IfZCmp implements Expression {
	private final Expression value;
	private final CompareOperation operation;
	private final Expression expressionTrue;
	private final Expression expressionFalse;

	Expression_IfZCmp(Expression value, CompareOperation operation, Expression expressionTrue, Expression expressionFalse) {
		this.value = value;
		this.operation = operation;
		this.expressionTrue = expressionTrue;
		this.expressionFalse = expressionFalse;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Label labelTrue = new Label();
		Label labelExit = new Label();

		Type valueType = value.load(ctx);
		checkType(valueType, type -> type != null && (type.getSort() >= BOOLEAN && type.getSort() <= INT));

		g.ifZCmp(operation.opCode, labelTrue);

		Type typeFalse = expressionFalse.load(ctx);
		g.goTo(labelExit);

		g.mark(labelTrue);
		Type typeTrue = expressionTrue.load(ctx);

		g.mark(labelExit);

		return ctx.unifyTypes(typeFalse, typeTrue);
	}
}
