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

final class ExpressionTryFinally implements Expression {
	private final Expression tryBlock;
	private final Expression finallyBlock;

	ExpressionTryFinally(Expression tryBlock, Expression finallyBlock) {
		this.tryBlock = tryBlock;
		this.finallyBlock = finallyBlock;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Label start = new Label();
		Label finish = new Label();
		Label handler = new Label();
		Label end = new Label();

		g.visitTryCatchBlock(start, finish, handler, null);

		g.mark(start);
		Type tryResult = tryBlock.load(ctx);

		int local = -1;
		if (tryResult != null && tryResult != Type.VOID_TYPE) {
			local = g.newLocal(tryResult);
			g.storeLocal(local);
		}

		g.mark(finish);
		Type finallyResult = finallyBlock.load(ctx);
		boolean notOverwritten = finallyResult == null || finallyResult == Type.VOID_TYPE;
		if (local != -1 && notOverwritten) {
			g.loadLocal(local);
		}
		g.goTo(end);

		g.mark(handler);

		int throwable = g.newLocal(Type.getType(Throwable.class));
		g.storeLocal(throwable);
		finallyBlock.load(ctx);
		if (notOverwritten) {
			g.loadLocal(throwable);
			g.throwException();
		}

		g.mark(end);

		if (finallyResult == Type.VOID_TYPE) {
			return tryResult;
		}

		return ctx.unifyTypes(tryResult, finallyResult);
	}
}
