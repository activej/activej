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
import io.activej.common.builder.AbstractBuilder;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.codegen.expression.Expressions.property;
import static io.activej.codegen.expression.Expressions.self;
import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isAssignable;
import static io.activej.codegen.util.Utils.*;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Defines methods which allow to create a string
 */
@ExplicitlyExposed
public final class ToStringExpression implements Expression {
	private String begin;
	private String end;
	private @Nullable String nameSeparator;
	private String valueSeparator;
	private final Map<Object, Expression> arguments;

	public ToStringExpression(String begin, String end, @Nullable String nameSeparator, String valueSeparator,
			Map<Object, Expression> arguments) {
		this.begin = begin;
		this.end = end;
		this.nameSeparator = nameSeparator;
		this.valueSeparator = valueSeparator;
		this.arguments = arguments;
	}

	public static ToStringExpression create() {
		return builder().build();
	}

	public static Builder builder() {
		return new ToStringExpression("{", "}", ": ", ", ", new LinkedHashMap<>()).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, ToStringExpression> {
		private Builder() {}

		public Builder withBeginTag(String begin) {
			checkNotBuilt(this);
			ToStringExpression.this.begin = begin;
			return this;
		}

		public Builder withEndTag(String end) {
			checkNotBuilt(this);
			ToStringExpression.this.end = end;
			return this;
		}

		public Builder withNameSeparator(@Nullable String nameSeparator) {
			checkNotBuilt(this);
			ToStringExpression.this.nameSeparator = nameSeparator;
			return this;
		}

		public Builder withValueSeparator(String valueSeparator) {
			checkNotBuilt(this);
			ToStringExpression.this.valueSeparator = valueSeparator;
			return this;
		}

		public Builder with(String label, Expression expression) {
			checkNotBuilt(this);
			ToStringExpression.this.arguments.put(label, expression);
			return this;
		}

		public Builder with(Expression expression) {
			checkNotBuilt(this);
			ToStringExpression.this.arguments.put(arguments.size() + 1, expression);
			return this;
		}

		public Builder withField(String field) {
			checkNotBuilt(this);
			ToStringExpression.this.arguments.put(field, property(self(), field));
			return this;
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
		protected ToStringExpression doBuild() {
			return ToStringExpression.this;
		}
	}

	public String getBegin() {
		return begin;
	}

	public String getEnd() {
		return end;
	}

	public @Nullable String getNameSeparator() {
		return nameSeparator;
	}

	public String getValueSeparator() {
		return valueSeparator;
	}

	public Map<Object, Expression> getArguments() {
		return arguments;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		g.newInstance(getType(StringBuilder.class));
		g.dup();
		g.invokeConstructor(getType(StringBuilder.class), getMethod("void <init> ()"));

		boolean first = true;

		for (Map.Entry<Object, Expression> entry : arguments.entrySet()) {
			String str = first ? begin : valueSeparator;
			first = false;
			if (entry.getKey() instanceof String) {
				if (nameSeparator != null) {
					str += entry.getKey() + nameSeparator;
				}
			}
			if (!str.isEmpty()) {
				g.dup();
				g.push(str);
				g.invokeVirtual(getType(StringBuilder.class), getMethod("StringBuilder append(String)"));
				g.pop();
			}

			g.dup();
			Type type = entry.getValue().load(ctx);
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
