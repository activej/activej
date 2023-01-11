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
import io.activej.codegen.operation.ArithmeticOperation;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static io.activej.codegen.operation.ArithmeticOperation.*;
import static io.activej.codegen.util.TypeChecks.*;
import static io.activej.codegen.util.Utils.isWrapperType;
import static io.activej.codegen.util.Utils.unwrap;
import static org.objectweb.asm.Type.getType;

final class Expression_ArithmeticOp implements Expression {
	private final ArithmeticOperation op;
	private final Expression left;
	private final Expression right;

	Expression_ArithmeticOp(ArithmeticOperation op, Expression left, Expression right) {
		this.op = op;
		this.left = left;
		this.right = right;
	}

	static Class<?> unifyArithmeticTypes(Class<?>... dataTypes) {
		Class<?> resultType = null;
		int maxOrder = 0;

		for (Class<?> dataType : dataTypes) {
			Class<?> t;
			int order;
			if (dataType == Byte.TYPE || dataType == Short.TYPE || dataType == Character.TYPE || dataType == Integer.TYPE) {
				t = Integer.TYPE;
				order = 1;
			} else {
				t = dataType;
				if (dataType == Long.TYPE) {
					order = 2;
				} else if (dataType == Float.TYPE) {
					order = 3;
				} else if (dataType == Double.TYPE) {
					order = 4;
				} else {
					throw new IllegalArgumentException("Not an arithmetic type: " + dataType);
				}
			}
			if (resultType == null || order > maxOrder) {
				resultType = t;
				maxOrder = order;
			}
		}

		return resultType;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Type leftType = left.load(ctx);
		checkType(leftType, isArithmetic().or(isWrapper()));

		if (isWrapperType(leftType)) {
			leftType = unwrap(leftType);
			g.unbox(leftType);
		}

		Type rightType = right.load(ctx);
		checkType(rightType, isArithmetic().or(isWrapper()));

		if (isWrapperType(rightType)) {
			rightType = unwrap(rightType);
			g.unbox(rightType);
		}

		if (op != SHL && op != SHR && op != USHR) {
			Type resultType = getType(unifyArithmeticTypes(ctx.toJavaType(leftType), ctx.toJavaType(rightType)));
			if (leftType != resultType) {
				int rightLocal = g.newLocal(rightType);
				g.storeLocal(rightLocal);
				g.cast(leftType, resultType);
				g.loadLocal(rightLocal);
			}
			if (rightType != resultType) {
				g.cast(rightType, resultType);
			}
			g.visitInsn(resultType.getOpcode(op.opCode));
			return resultType;
		} else {
			if (rightType != Type.getType(int.class)) {
				g.cast(rightType, Type.getType(int.class));
			}
			g.visitInsn(leftType.getOpcode(op.opCode));
			return leftType;
		}
	}
}
