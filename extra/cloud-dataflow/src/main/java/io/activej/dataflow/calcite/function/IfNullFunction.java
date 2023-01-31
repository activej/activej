package io.activej.dataflow.calcite.function;

import io.activej.dataflow.calcite.operand.FunctionOperand;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.operand.Operand_IfNull;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.util.List;

import static io.activej.common.Checks.checkArgument;
import static org.apache.calcite.sql.type.OperandTypes.family;

public final class IfNullFunction extends ProjectionFunction {
	public IfNullFunction() {
		super("IFNULL", SqlKind.OTHER_FUNCTION, opBinding -> opBinding.getOperandType(1),
				InferTypes.ANY_NULLABLE, family(SqlTypeFamily.ANY, SqlTypeFamily.ANY), SqlFunctionCategory.USER_DEFINED_FUNCTION);
	}

	@Override
	public FunctionOperand<?> toOperandFunction(List<Operand<?>> operands) {
		checkArgument(operands.size() == 2);

		Operand<?> checkedOperand = operands.get(0);
		Operand<?> defaultValueOperand = operands.get(1);
		return new Operand_IfNull(checkedOperand, defaultValueOperand);
	}
}
