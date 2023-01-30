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
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Type;

public final class Expression_Let implements Variable {
	private final Expression value;

	Expression_Let(Expression value) {
		this.value = value;
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
