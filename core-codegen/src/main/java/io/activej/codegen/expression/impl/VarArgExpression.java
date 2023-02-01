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
import io.activej.codegen.expression.Variable;
import io.activej.common.annotation.ExplicitlyExposed;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Type;

/**
 * Defines method which allow taking argument according to their ordinal number
 */
@ExplicitlyExposed
public final class VarArgExpression implements Variable {
	private final int argument;

	public VarArgExpression(int argument) {
		this.argument = argument;
	}

	public int getArgument() {
		return argument;
	}

	@Override
	public Type load(Context ctx) {
		ctx.getGeneratorAdapter().loadArg(argument);
		return ctx.getGeneratorAdapter().getArgumentTypes()[argument];
	}

	@Override
	public @Nullable Object beginStore(Context ctx) {
		return null;
	}

	@Override
	public void store(Context ctx, Object storeContext, Type type) {
		ctx.getGeneratorAdapter().storeArg(argument);
	}
}
