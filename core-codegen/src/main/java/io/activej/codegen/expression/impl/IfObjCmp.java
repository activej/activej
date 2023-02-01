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
import io.activej.codegen.operation.CompareOperation;
import io.activej.common.annotation.ExplicitlyExposed;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import static io.activej.codegen.operation.CompareOperation.*;
import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isAssignable;
import static io.activej.codegen.util.Utils.invokeVirtualOrInterface;
import static io.activej.codegen.util.Utils.isPrimitiveType;
import static org.objectweb.asm.Type.BOOLEAN_TYPE;
import static org.objectweb.asm.Type.INT_TYPE;

/**
 * Defines methods for comparing functions
 */
@ExplicitlyExposed
public final class IfObjCmp implements Expression {
	private final Expression left;
	private final Expression right;
	private final CompareOperation operation;
	private final Expression expressionTrue;
	private final Expression expressionFalse;

	public IfObjCmp(CompareOperation operation, Expression left, Expression right, Expression expressionTrue, Expression expressionFalse) {
		this.left = left;
		this.right = right;
		this.operation = operation;
		this.expressionTrue = expressionTrue;
		this.expressionFalse = expressionFalse;
	}

	public Expression getLeft() {
		return left;
	}

	public Expression getRight() {
		return right;
	}

	public CompareOperation getOperation() {
		return operation;
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

		Type leftType = left.load(ctx);
		checkType(leftType, isAssignable());

		Type rightType = right.load(ctx);
		checkType(rightType, isAssignable());

		if (!leftType.equals(rightType))
			throw new IllegalArgumentException("Types of compared values should match");

		if (isPrimitiveType(leftType)) {
			g.ifCmp(leftType, operation.opCode, labelTrue);
		} else {
			if (operation == REF_EQ || operation == REF_NE) {
				g.ifCmp(leftType, operation.opCode, labelTrue);
			} else if (operation == EQ || operation == NE) {
				invokeVirtualOrInterface(ctx, leftType, new Method("equals", BOOLEAN_TYPE, new Type[]{Type.getType(Object.class)}));
				g.ifZCmp(operation == EQ ? GeneratorAdapter.NE : GeneratorAdapter.EQ, labelTrue);
			} else {
				invokeVirtualOrInterface(ctx, leftType, new Method("compareTo", INT_TYPE, new Type[]{Type.getType(Object.class)}));
				g.ifZCmp(operation.opCode, labelTrue);
			}
		}

		Type typeFalse = expressionFalse.load(ctx);
		g.goTo(labelExit);

		g.mark(labelTrue);
		Type typeTrue = expressionTrue.load(ctx);

		g.mark(labelExit);

		return ctx.unifyTypes(typeFalse, typeTrue);
	}
}
