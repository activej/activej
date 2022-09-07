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

package io.activej.dataflow.proto.calcite.serializer;

import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.operand.OperandFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.activej.common.Checks.checkArgument;

public final class OperandFunctionRegistry {
	private static final Map<String, OperandFunctionFactory<?>> NAMES_TO_FACTORIES = new HashMap<>();

	private OperandFunctionRegistry() {
	}

	public static <T extends OperandFunction<T>> void register(Class<T> cls, OperandFunctionFactory<T> factory) {
		OperandFunctionFactory<?> prev = NAMES_TO_FACTORIES.put(cls.getName(), factory);
		checkArgument(prev == null, "Factory for operand function " + cls.getName() + " is already registered");
	}

	public static <T extends OperandFunction<T>> OperandFunctionFactory<T> getFactory(String functionName) {
		//noinspection unchecked
		return (OperandFunctionFactory<T>) NAMES_TO_FACTORIES.get(functionName);
	}

	public interface OperandFunctionFactory<T extends OperandFunction<T>> {
		T create(List<Operand<?>> operands);
	}
}
