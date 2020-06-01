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

import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static io.activej.common.Preconditions.checkArgument;
import static org.objectweb.asm.Type.getType;

final class ExpressionArrayNew implements Expression {
	private final Class<?> type;
	private final Expression length;

	ExpressionArrayNew(Class<?> type, Expression length) {
		this.type = checkArgument(type, Class::isArray);
		this.length = length;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		length.load(ctx);
		g.newArray(getType(getType(type).getDescriptor().substring(1)));
		return getType(type);
	}
}
