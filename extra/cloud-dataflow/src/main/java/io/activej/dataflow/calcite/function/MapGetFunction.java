package io.activej.dataflow.calcite.function;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;

import static org.apache.calcite.sql.type.OperandTypes.family;

public final class MapGetFunction extends SqlFunction {

	public MapGetFunction() {
		super("MAP_GET", SqlKind.OTHER_FUNCTION, opBinding -> opBinding.getOperandType(0).getValueType(),
				null, family(SqlTypeFamily.MAP, SqlTypeFamily.ANY), SqlFunctionCategory.USER_DEFINED_FUNCTION);
	}
}
