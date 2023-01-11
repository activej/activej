package io.activej.dataflow.calcite.function;

import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.operand.Operand_Function;
import io.activej.dataflow.calcite.operand.Operand_MapGet;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;

import java.util.List;

import static io.activej.common.Checks.checkArgument;
import static org.apache.calcite.sql.type.OperandTypes.family;

public final class MapGetFunction extends ProjectionFunction {
	public MapGetFunction() {
		super("MAP_GET", SqlKind.OTHER_FUNCTION, ((SqlReturnTypeInference) (opBinding -> opBinding.getOperandType(0).getValueType()))
						.andThen(SqlTypeTransforms.FORCE_NULLABLE),
				InferTypes.ANY_NULLABLE, family(SqlTypeFamily.MAP, SqlTypeFamily.ANY), SqlFunctionCategory.USER_DEFINED_FUNCTION);
	}

	@Override
	public Operand_Function<?> toOperandFunction(List<Operand<?>> operands) {
		checkArgument(operands.size() == 2);

		Operand<?> mapOperand = operands.get(0);
		Operand<?> keyOperand = operands.get(1);
		return new Operand_MapGet(mapOperand, keyOperand);
	}
}
