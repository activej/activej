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
import org.objectweb.asm.commons.TableSwitchGenerator;

import java.util.Arrays;

import static org.objectweb.asm.Type.INT_TYPE;

class ExpressionTableSwitch implements Expression {
	private final Expression value;
	private final int[] keys;
	private final Expression[] matchExpressions;
	private final Expression defaultExpression;

	ExpressionTableSwitch(Expression value, int[] keys, Expression[] matchExpressions, Expression defaultExpression) {
		this.value = value;
		this.keys = keys;
		this.matchExpressions = matchExpressions;
		this.defaultExpression = defaultExpression;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Type keyType = this.value.load(ctx);
		if (keyType != INT_TYPE) throw new IllegalArgumentException("Key expression must be int " + keyType);

		class TypeRef {
			Type type;
		}

		TypeRef returnTypeRef = new TypeRef();
		g.tableSwitch(keys, new TableSwitchGenerator() {
			@Override
			public void generateCase(int key, Label end) {
				int idx = Arrays.binarySearch(keys, key);
				Type type = matchExpressions[idx].load(ctx);
				g.goTo(end);
				returnTypeRef.type = type;
			}

			@Override
			public void generateDefault() {
				returnTypeRef.type = defaultExpression.load(ctx);
			}
		});

		return returnTypeRef.type;
	}
}
