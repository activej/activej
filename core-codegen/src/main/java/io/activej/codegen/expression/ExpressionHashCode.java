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

import java.util.ArrayList;
import java.util.List;

import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isAssignable;
import static io.activej.codegen.util.Utils.isPrimitiveType;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.INT_TYPE;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Defines methods for hashing some fields
 */
public final class ExpressionHashCode implements Expression {
	private final List<Expression> arguments = new ArrayList<>();

	private ExpressionHashCode() {
	}

	public static ExpressionHashCode create() {
		return new ExpressionHashCode();
	}

	public ExpressionHashCode with(Expression expression) {
		this.arguments.add(expression);
		return this;
	}

	public ExpressionHashCode withField(String field) {
		return with(Expressions.property(Expressions.self(), field));
	}

	public ExpressionHashCode withFields(List<String> fields) {
		for (String field : fields) {
			withField(field);
		}
		return this;
	}

	public ExpressionHashCode withFields(String... fields) {
		return withFields(List.of(fields));
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		int resultVar = g.newLocal(INT_TYPE);

		boolean firstIteration = true;

		for (Expression argument : arguments) {
			if (firstIteration) {
				g.push(0);
				firstIteration = false;
			} else {
				g.push(31);
				g.loadLocal(resultVar);
				g.math(IMUL, INT_TYPE);
			}

			Type fieldType = argument.load(ctx);
			checkType(fieldType, isAssignable());

			if (isPrimitiveType(fieldType)) {
				if (fieldType.getSort() == Type.LONG) {
					g.dup2();
					g.push(32);
					g.visitInsn(LUSHR);
					g.visitInsn(LXOR);
					g.visitInsn(L2I);
				}
				if (fieldType.getSort() == Type.FLOAT) {
					g.invokeStatic(getType(Float.class), getMethod("int floatToRawIntBits (float)"));
				}
				if (fieldType.getSort() == Type.DOUBLE) {
					g.invokeStatic(getType(Double.class), getMethod("long doubleToRawLongBits (double)"));
					g.dup2();
					g.push(32);
					g.visitInsn(LUSHR);
					g.visitInsn(LXOR);
					g.visitInsn(L2I);
				}
				g.visitInsn(IADD);
			} else {
				int tmpVar = g.newLocal(fieldType);
				g.storeLocal(tmpVar);
				g.loadLocal(tmpVar);
				Label ifNullLabel = g.newLabel();
				g.ifNull(ifNullLabel);
				g.loadLocal(tmpVar);
				g.invokeVirtual(fieldType, getMethod("int hashCode()"));
				g.visitInsn(IADD);
				g.mark(ifNullLabel);
			}

			g.storeLocal(resultVar);
		}

		if (firstIteration) {
			g.push(0);
		} else {
			g.loadLocal(resultVar);
		}

		return INT_TYPE;
	}
}
