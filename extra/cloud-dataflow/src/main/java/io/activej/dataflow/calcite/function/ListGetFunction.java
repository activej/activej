package io.activej.dataflow.calcite.function;

import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.operand.OperandListGet;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.util.List;

import static io.activej.common.Checks.checkArgument;
import static org.apache.calcite.sql.type.OperandTypes.family;

public final class ListGetFunction extends ProjectionFunction {
	public ListGetFunction() {
		super("LIST_GET", SqlKind.OTHER_FUNCTION, opBinding -> opBinding.getOperandType(0).getComponentType(),
				InferTypes.ANY_NULLABLE,
				OperandTypes.or(OperandTypes.ANY,
						family(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER),
						family(SqlTypeFamily.ARRAY, SqlTypeFamily.ANY)),
				SqlFunctionCategory.USER_DEFINED_FUNCTION);
	}

	@Override
	public Operand<?> toOperand(List<Operand<?>> operands) {
		checkArgument(operands.size() == 2);

		Operand<?> listOperand = operands.get(0);
		Operand<?> indexOperand = operands.get(1);
		return new OperandListGet(listOperand, indexOperand);
	}
}
