package io.activej.dataflow.calcite.function;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;

import static org.apache.calcite.sql.type.OperandTypes.family;

public final class ListGetFunction extends SqlFunction {

	public ListGetFunction() {
		super("LIST_GET", SqlKind.OTHER_FUNCTION, opBinding -> opBinding.getOperandType(0).getComponentType(),
				null, family(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER), SqlFunctionCategory.USER_DEFINED_FUNCTION);
	}
}
