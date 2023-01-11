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

final class Expression_Call implements Expression {
	private final Expression owner;
	private final String methodName;
	private final Expression[] arguments;

	Expression_Call(Expression owner, String methodName, Expression[] arguments) {
		this.owner = owner;
		this.methodName = methodName;
		this.arguments = arguments;
	}

	@Override
	public Type load(Context ctx) {
		return ctx.invoke(owner, methodName, arguments);
	}
}
