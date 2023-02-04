package io.activej.dataflow.calcite.operand;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.annotation.StaticFactories;
import io.activej.dataflow.calcite.Value;
import io.activej.dataflow.calcite.operand.impl.*;

@StaticFactories(Operand.class)
public class Operands {
	public static Operand<?> cast(Operand<?> operand, int type) {
		return new Cast(operand, type);
	}

	public static Operand<?> fieldAccess(Operand<?> objectOperand, String fieldName, DefiningClassLoader classLoader) {
		return new FieldAccess(objectOperand, fieldName, classLoader);
	}

	public static Operand<?> recordField(int index) {
		return new RecordField(index);
	}

	public static Operand<?> scalar(Value value) {
		return new Scalar(value);
	}

	public static FunctionOperand<?> ifNull(Operand<?> checkedOperand, Operand<?> defaultValueOperand) {
		return new IfNull(checkedOperand, defaultValueOperand);
	}

	public static FunctionOperand<?> listGet(Operand<?> listOperand, Operand<?> indexOperand) {
		return new ListGet(listOperand, indexOperand);
	}

	public static FunctionOperand<?> mapGet(Operand<?> mapOperand, Operand<?> keyOperand) {
		return new MapGet(mapOperand, keyOperand);
	}
}
