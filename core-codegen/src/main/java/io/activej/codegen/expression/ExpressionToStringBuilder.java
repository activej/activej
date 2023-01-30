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
final class Expression_ToString implements Expression {
	private String begin = "{";
	private String end = "}";
	private @Nullable String nameSeparator = ": ";
	private String valueSeparator = ", ";
	private final Map<Object, Expression> arguments = new LinkedHashMap<>();

	private Expression_ToString() {
	}

	static Builder builder() {
		return new Expression_ToString().new Builder();
	}

	final class Builder extends AbstractBuilder<Builder, Expression>
			implements ExpressionToStringBuilder {
		Builder() {}

		@Override
		public Builder withBeginTag(String begin) {
			checkNotBuilt(this);
			Expression_ToString.this.begin = begin;
			return this;
		}

		@Override
		public Builder withEndTag(String end) {
			checkNotBuilt(this);
			Expression_ToString.this.end = end;
			return this;
		}

		@Override
		public Builder withNameSeparator(String nameSeparator) {
			checkNotBuilt(this);
			Expression_ToString.this.nameSeparator = nameSeparator;
			return this;
		}

		@Override
		public Builder withValueSeparator(String valueSeparator) {
			checkNotBuilt(this);
			Expression_ToString.this.valueSeparator = valueSeparator;
			return this;
		}

		@Override
		public Builder with(String label, Expression expression) {
			checkNotBuilt(this);
			Expression_ToString.this.arguments.put(label, expression);
			return this;
		}

		@Override
		public Builder with(Expression expression) {
			checkNotBuilt(this);
			Expression_ToString.this.arguments.put(arguments.size() + 1, expression);
			return this;
		}

		@Override
		public Builder withField(String field) {
			checkNotBuilt(this);
			Expression_ToString.this.arguments.put(field, property(self(), field));
			return this;
		}

		@Override
		public Builder withFields(List<String> fields) {
			checkNotBuilt(this);
			for (String field : fields) {
				withField(field);
			}
			return this;
		}

		@Override
		public Builder withFields(String... fields) {
			checkNotBuilt(this);
			return withFields(List.of(fields));
		}

		@Override
		protected Expression doBuild() {
			return Expression_ToString.this;
		}
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

public interface ExpressionToStringBuilder extends Builder<Expression> {
	ExpressionToStringBuilder withBeginTag(String begin);

	ExpressionToStringBuilder withEndTag(String end);

	ExpressionToStringBuilder withNameSeparator(String nameSeparator);

	ExpressionToStringBuilder withValueSeparator(String valueSeparator);

	ExpressionToStringBuilder with(String label, Expression expression);

	ExpressionToStringBuilder with(Expression expression);

	ExpressionToStringBuilder withField(String field);

	ExpressionToStringBuilder withFields(List<String> fields);

	ExpressionToStringBuilder withFields(String... fields);
}
