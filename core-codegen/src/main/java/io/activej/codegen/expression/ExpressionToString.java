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

import java.util.LinkedHashMap;
import java.util.Map;

import static io.activej.codegen.util.Utils.isPrimitiveType;
import static io.activej.codegen.util.Utils.wrap;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Defines methods which allow to create a string
 */
public final class ExpressionToString implements Expression {
	private String begin = "{";
	private String end = "}";
	private String separator = " ";
	private final Map<Object, Expression> arguments = new LinkedHashMap<>();

	ExpressionToString() {
	}

	public static ExpressionToString create() {
		return new ExpressionToString();
	}

	public ExpressionToString with(String label, Expression expression) {
		this.arguments.put(label, expression);
		return this;
	}

	public ExpressionToString with(Expression expression) {
		this.arguments.put(arguments.size() + 1, expression);
		return this;
	}

	public ExpressionToString withSeparator(String separator) {
		this.separator = separator;
		return this;
	}

	public ExpressionToString withQuotes(String begin, String end) {
		this.begin = begin;
		this.end = end;
		return this;
	}

	public ExpressionToString withQuotes(String begin, String end, String separator) {
		this.begin = begin;
		this.end = end;
		this.separator = separator;
		return this;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		g.newInstance(getType(StringBuilder.class));
		g.dup();
		g.invokeConstructor(getType(StringBuilder.class), getMethod("void <init> ()"));

		boolean first = true;

		for (Map.Entry<Object, Expression> entry : arguments.entrySet()) {
			String str = first ? begin : separator;
			first = false;
			if (entry.getKey() instanceof String) {
				str += entry.getKey();
			}
			if (!str.isEmpty()) {
				g.dup();
				g.push(str);
				g.invokeVirtual(getType(StringBuilder.class), getMethod("StringBuilder append(String)"));
				g.pop();
			}

			g.dup();
			Type type = entry.getValue().load(ctx);
			if (isPrimitiveType(type)) {
				g.invokeStatic(wrap(type), new Method("toString", getType(String.class), new Type[]{type}));
			} else {
				Label nullLabel = new Label();
				Label afterToString = new Label();
				g.dup();
				g.ifNull(nullLabel);
				g.invokeVirtual(type, getMethod("String toString()"));
				g.goTo(afterToString);
				g.mark(nullLabel);
				g.pop();
				g.push("null");
				g.mark(afterToString);
			}
			g.invokeVirtual(getType(StringBuilder.class), getMethod("StringBuilder append(String)"));
			g.pop();
		}

		if (!end.isEmpty()) {
			g.dup();
			g.push(end);
			g.invokeVirtual(getType(StringBuilder.class), getMethod("StringBuilder append(String)"));
			g.pop();
		}

		g.invokeVirtual(getType(StringBuilder.class), getMethod("String toString()"));
		return getType(String.class);
	}
}
