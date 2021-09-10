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
import org.objectweb.asm.Type;

/**
 * Defines methods which allow setting fields
 */
final class ExpressionSet implements Expression {
	private final StoreDef to;
	private final Expression from;

	ExpressionSet(StoreDef to, Expression from) {
		this.to = to;
		this.from = from;
	}

	@Override
	public Type load(Context ctx) {
		Object storeContext = to.beginStore(ctx);
		Type type = from.load(ctx);
		to.store(ctx, storeContext, type);
		return Type.VOID_TYPE;
	}
}
