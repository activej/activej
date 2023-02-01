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
import io.activej.codegen.expression.StoreDef;
import io.activej.common.annotation.ExplicitlyExposed;
import org.objectweb.asm.Type;

import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isAssignable;

/**
 * Defines methods which allow setting fields
 */
@ExplicitlyExposed
public final class SetExpression implements Expression {
	private final StoreDef to;
	private final Expression from;

	public SetExpression(StoreDef to, Expression from) {
		this.to = to;
		this.from = from;
	}

	public StoreDef getTo() {
		return to;
	}

	public Expression getFrom() {
		return from;
	}

	@Override
	public Type load(Context ctx) {
		Object storeContext = to.beginStore(ctx);
		Type type = from.load(ctx);
		checkType(type, isAssignable());

		to.store(ctx, storeContext, type);
		return Type.VOID_TYPE;
	}
}
