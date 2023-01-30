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
import org.objectweb.asm.commons.Method;

import java.util.List;

import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isAssignable;
import static io.activej.codegen.util.Utils.*;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Defines methods which allow to concat arguments into a string
 */
final class Expression_Concat implements Expression {
	private final List<Expression> arguments;

	Expression_Concat(List<Expression> arguments) {
		this.arguments = arguments;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		g.newInstance(getType(StringBuilder.class));
		g.dup();
		g.invokeConstructor(getType(StringBuilder.class), getMethod("void <init> ()"));

		for (Expression expression : arguments) {
			g.dup();
			Type type = expression.load(ctx);
			checkType(type, isAssignable());

			if (isPrimitiveType(type)) {
				g.invokeStatic(wrap(type), new Method("toString", getType(String.class), new Type[]{type}));
			} else {
				Label nullLabel = new Label();
				Label afterToString = new Label();
				g.dup();
				g.ifNull(nullLabel);
				invokeVirtualOrInterface(ctx, type, getMethod("String toString()"));
				g.goTo(afterToString);
				g.mark(nullLabel);
				g.pop();
				g.push("null");
				g.mark(afterToString);
			}
			g.invokeVirtual(getType(StringBuilder.class), getMethod("StringBuilder append(String)"));
			g.pop();
		}

		g.invokeVirtual(getType(StringBuilder.class), getMethod("String toString()"));
		return getType(String.class);
	}
}
