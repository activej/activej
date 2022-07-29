package io.activej.dataflow.calcite.function;

import io.activej.dataflow.calcite.where.Operand;
import io.activej.dataflow.calcite.where.OperandMapGet;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.util.List;

import static io.activej.common.Checks.checkArgument;
import static org.apache.calcite.sql.type.OperandTypes.family;

public final class MapGetFunction extends ProjectionFunction {
	public MapGetFunction() {
		super("MAP_GET", SqlKind.OTHER_FUNCTION, opBinding -> opBinding.getOperandType(0).getValueType(),
				InferTypes.ANY_NULLABLE, family(SqlTypeFamily.MAP, SqlTypeFamily.ANY), SqlFunctionCategory.USER_DEFINED_FUNCTION);
	}

	@Override
	public Operand toOperand(List<Operand> operands) {
		checkArgument(operands.size() == 2);

		Operand mapOperand = operands.get(0);
		Operand keyOperand = operands.get(1);
		return new OperandMapGet<>(mapOperand, keyOperand);
	}
}
