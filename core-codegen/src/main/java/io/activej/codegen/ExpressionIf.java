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

package io.activej.codegen;

import org.jetbrains.annotations.NotNull;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

final class ExpressionIf implements Expression {
	private final Expression condition;
	private final Expression left;
	private final Expression right;

	ExpressionIf(@NotNull Expression condition, @NotNull Expression left, @NotNull Expression right) {
		this.condition = condition;
		this.left = left;
		this.right = right;
	}

	@Override
	public Type load(Context ctx) {
		Label labelTrue = new Label();
		Label labelExit = new Label();

		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Type conditionType = condition.load(ctx);
		g.push(true);

		g.ifCmp(conditionType, GeneratorAdapter.EQ, labelTrue);

		right.load(ctx);

		g.goTo(labelExit);

		g.mark(labelTrue);
		Type leftType = left.load(ctx);

		g.mark(labelExit);
		return leftType;
	}
}
