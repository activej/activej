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
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.builder.Builder;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.util.ArrayList;
import java.util.List;

import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isAssignable;
import static io.activej.codegen.util.Utils.*;
import static org.objectweb.asm.Type.INT_TYPE;
import static org.objectweb.asm.commons.GeneratorAdapter.NE;

/**
 * Defines methods to compare some fields
 */
final class Expression_Compare implements Expression {
	private final List<Pair> pairs = new ArrayList<>();

	private record Pair(Expression left, Expression right, boolean nullable) {}

	private Expression_Compare() {
	}

	static Builder builder() {
		return new Expression_Compare().new Builder();
	}

	final class Builder extends AbstractBuilder<Builder, Expression>
			implements ExpressionCompareBuilder {
		Builder() {}

		@Override
		public Builder with(Expression left, Expression right) {
			checkNotBuilt(this);
			return with(left, right, false);
		}

		@Override
		public Builder with(Expression left, Expression right, boolean nullable) {
			checkNotBuilt(this);
			Expression_Compare.this.pairs.add(new Expression_Compare.Pair(left, right, nullable));
			return this;
		}

		@Override
		protected Expression doBuild() {
			return Expression_Compare.this;
		}
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Label labelReturn = new Label();

		for (Pair pair : pairs) {
			Type leftPropertyType = pair.left.load(ctx);
			checkType(leftPropertyType, isAssignable());

			Type rightPropertyType = pair.right.load(ctx);
			checkType(rightPropertyType, isAssignable());

			if (!leftPropertyType.equals(rightPropertyType))
				throw new IllegalArgumentException("Types of compared values should match");
			if (isPrimitiveType(leftPropertyType)) {
				g.invokeStatic(wrap(leftPropertyType), new Method("compare", INT_TYPE, new Type[]{leftPropertyType, leftPropertyType}));
				g.dup();
				g.ifZCmp(NE, labelReturn);
				g.pop();
			} else if (!pair.nullable) {
				invokeVirtualOrInterface(ctx, leftPropertyType, new Method("compareTo", INT_TYPE, new Type[]{Type.getType(Object.class)}));
				g.dup();
				g.ifZCmp(NE, labelReturn);
				g.pop();
			} else {
				LocalVariable varRight = ctx.newLocal(rightPropertyType);
				varRight.store(ctx);

				LocalVariable varLeft = ctx.newLocal(leftPropertyType);
				varLeft.store(ctx);

				Label continueLabel = new Label();
				Label nonNulls = new Label();
				Label leftNonNull = new Label();

				varLeft.load(ctx);
				g.ifNonNull(leftNonNull);

				varRight.load(ctx);
				g.ifNull(continueLabel);
				g.push(-1);
				g.returnValue();

				g.mark(leftNonNull);

				varRight.load(ctx);
				g.ifNonNull(nonNulls);
				g.push(1);
				g.returnValue();

				g.mark(nonNulls);

				varLeft.load(ctx);
				varRight.load(ctx);

				invokeVirtualOrInterface(ctx, leftPropertyType, new Method("compareTo", INT_TYPE, new Type[]{Type.getType(Object.class)}));
				g.dup();
				g.ifZCmp(NE, labelReturn);
				g.pop();

				g.mark(continueLabel);
			}
		}

		g.push(0);

		g.mark(labelReturn);

		return INT_TYPE;
	}
}

public interface ExpressionCompareBuilder extends Builder<Expression> {
	ExpressionCompareBuilder with(Expression left, Expression right);

	ExpressionCompareBuilder with(Expression left, Expression right, boolean nullable);
}

