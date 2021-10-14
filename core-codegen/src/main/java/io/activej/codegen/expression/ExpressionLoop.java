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

import static org.objectweb.asm.Type.BOOLEAN_TYPE;

public class ExpressionLoop implements Expression {
	protected final Expression body;

	public ExpressionLoop(Expression body) {this.body = body;}

	@Override
	public final Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label labelLoop = new Label();
		Label labelExit = new Label();

		g.mark(labelLoop);

		body.load(ctx);
		g.push(false);
		g.ifCmp(BOOLEAN_TYPE, GeneratorAdapter.EQ, labelExit);

		g.goTo(labelLoop);
		g.mark(labelExit);
		return Type.VOID_TYPE;
	}
}
