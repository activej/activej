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
import io.activej.codegen.expression.LocalVariable;
import io.activej.codegen.expression.Variable;
import io.activej.common.annotation.ExplicitlyExposed;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Type;

@ExplicitlyExposed
public final class Expression_Let implements Variable {
	private final Expression value;

	public Expression_Let(Expression value) {
		this.value = value;
	}

	public Expression getValue() {
		return value;
	}

	@Override
	public Type load(Context ctx) {
		LocalVariable var = ctx.ensureLocal(this, value);
		return var.load(ctx);
	}

	@Override
	public @Nullable Object beginStore(Context ctx) {
		return null;
	}

	@Override
	public void store(Context ctx, Object storeContext, Type type) {
		LocalVariable var = ctx.ensureLocal(this, value);
		var.store(ctx, storeContext, type);
	}
}
