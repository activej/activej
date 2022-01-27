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

package io.activej.codegen.operation;

import org.objectweb.asm.commons.GeneratorAdapter;

public enum CompareOperation {
	EQ(GeneratorAdapter.EQ, "=="),
	NE(GeneratorAdapter.NE, "!="),
	REF_EQ(GeneratorAdapter.EQ, "==="),
	REF_NE(GeneratorAdapter.NE, "!=="),
	LT(GeneratorAdapter.LT, "<"),
	GT(GeneratorAdapter.GT, ">"),
	LE(GeneratorAdapter.LE, "<="),
	GE(GeneratorAdapter.GE, ">=");

	public final int opCode;
	public final String symbol;

	CompareOperation(int opCode, String symbol) {
		this.opCode = opCode;
		this.symbol = symbol;
	}

	public static CompareOperation operation(String symbol) {
		for (CompareOperation operation : values()) {
			if (operation.symbol.equals(symbol)) {
				return operation;
			}
		}
		throw new IllegalArgumentException("Did not found operation for symbol " + symbol);
	}
}
