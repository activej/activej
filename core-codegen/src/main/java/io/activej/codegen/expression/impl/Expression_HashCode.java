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
import io.activej.codegen.expression.Expressions;
import io.activej.common.annotation.ExplicitlyExposed;
import io.activej.common.builder.AbstractBuilder;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.ArrayList;
import java.util.List;

import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isAssignable;
import static io.activej.codegen.util.Utils.invokeVirtualOrInterface;
import static io.activej.codegen.util.Utils.isPrimitiveType;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.INT_TYPE;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Defines methods for hashing some fields
 */
@ExplicitlyExposed
public final class Expression_HashCode implements Expression {
	private final List<Expression> arguments;

	public Expression_HashCode(List<Expression> arguments) {
		this.arguments = arguments;
	}

	public static Builder builder() {
		return new Expression_HashCode(new ArrayList<>()).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, Expression_HashCode> {
		private Builder() {}

		public Builder with(Expression expression) {
			checkNotBuilt(this);
			Expression_HashCode.this.arguments.add(expression);
			return this;
		}

		public Builder withField(String field) {
			checkNotBuilt(this);
			return with(Expressions.property(Expressions.self(), field));
		}

		public Builder withFields(List<String> fields) {
			checkNotBuilt(this);
			for (String field : fields) {
				withField(field);
			}
			return this;
		}

		public Builder withFields(String... fields) {
			checkNotBuilt(this);
			return withFields(List.of(fields));
		}

		@Override
		protected Expression_HashCode doBuild() {
			return Expression_HashCode.this;
		}
	}

	public List<Expression> getArguments() {
		return arguments;
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
				invokeVirtualOrInterface(ctx, fieldType, getMethod("int hashCode()"));
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
