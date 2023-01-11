package io.activej.dataflow.calcite.function;

import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.operand.Operand_Function;
import io.activej.dataflow.calcite.operand.Operand_ListGet;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.*;

import java.util.List;

import static io.activej.common.Checks.checkArgument;
import static org.apache.calcite.sql.type.OperandTypes.family;

public final class ListGetFunction extends ProjectionFunction {
	public ListGetFunction() {
		super("LIST_GET", SqlKind.OTHER_FUNCTION, ((SqlReturnTypeInference) (opBinding -> opBinding.getOperandType(0).getComponentType()))
						.andThen(SqlTypeTransforms.FORCE_NULLABLE),
				InferTypes.ANY_NULLABLE,
				OperandTypes.or(OperandTypes.ANY,
						family(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER),
						family(SqlTypeFamily.ARRAY, SqlTypeFamily.ANY)),
				SqlFunctionCategory.USER_DEFINED_FUNCTION);
	}

	@Override
	public Operand_Function<?> toOperandFunction(List<Operand<?>> operands) {
		checkArgument(operands.size() == 2);

		Operand<?> listOperand = operands.get(0);
		Operand<?> indexOperand = operands.get(1);
		return new Operand_ListGet(listOperand, indexOperand);
	}
}
