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

import java.util.Map;
import java.util.function.BinaryOperator;

import static io.activej.codegen.expression.Expressions.call;

final class ExpressionMapForEach extends AbstractExpressionMapForEach {

	ExpressionMapForEach(Expression collection, BinaryOperator<Expression> action) {
		super(collection, action, Map.Entry.class);
	}

	@Override
	protected Expression getEntries() {
		return call(collection, "entrySet");
	}

	@Override
	protected Expression getKey(VarLocal entry) {
		return call(entry, "getKey");
	}

	@Override
	protected Expression getValue(VarLocal entry) {
		return call(entry, "getValue");
	}
}
