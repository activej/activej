package io.activej.dataflow.calcite.function;

import io.activej.dataflow.calcite.operand.FunctionOperand;
import io.activej.dataflow.calcite.operand.Operand;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public abstract class ProjectionFunction extends SqlFunction {
	protected ProjectionFunction(String name, SqlKind kind, @Nullable SqlReturnTypeInference returnTypeInference,
			@Nullable SqlOperandTypeInference operandTypeInference, @Nullable SqlOperandTypeChecker operandTypeChecker, SqlFunctionCategory category) {
		super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
	}

	public abstract FunctionOperand<?> toOperandFunction(List<Operand<?>> operands);
}
