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

import static org.objectweb.asm.Opcodes.*;

public enum ArithmeticOperation {
	ADD(IADD, "+"),
	SUB(ISUB, "-"),
	MUL(IMUL, "*"),
	DIV(IDIV, "/"),
	REM(IREM, "%"),
	AND(IAND, "&"),
	OR(IOR, "|"),
	XOR(IXOR, "^"),
	SHL(ISHL, "<<"),
	SHR(ISHR, ">>"),
	USHR(IUSHR, ">>>");

	public final int opCode;
	public final String symbol;

	ArithmeticOperation(int opCode, String symbol) {
		this.opCode = opCode;
		this.symbol = symbol;
	}

	public static ArithmeticOperation operation(String symbol) {
		for (ArithmeticOperation operation : ArithmeticOperation.values()) {
			if (operation.symbol.equals(symbol)) {
				return operation;
			}
		}
		throw new IllegalArgumentException("Did not found operation for symbol " + symbol);
	}
}
