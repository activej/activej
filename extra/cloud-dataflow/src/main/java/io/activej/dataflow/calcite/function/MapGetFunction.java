package io.activej.dataflow.calcite.function;

import io.activej.dataflow.calcite.RecordProjectionFn;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjectionMapGet;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static io.activej.dataflow.calcite.Utils.toJavaType;
import static org.apache.calcite.sql.type.OperandTypes.family;

public final class MapGetFunction extends ProjectionFunction {

	public MapGetFunction() {
		super("MAP_GET", SqlKind.OTHER_FUNCTION, opBinding -> opBinding.getOperandType(0).getValueType(),
				null, family(SqlTypeFamily.MAP, SqlTypeFamily.ANY), SqlFunctionCategory.USER_DEFINED_FUNCTION);
	}

	@Override
	public RecordProjectionFn.FieldProjection projectField(@Nullable String fieldName, List<RexNode> operands) {
		RexInputRef mapInput = (RexInputRef) operands.get(0);
		RexNode key = operands.get(1);

		return new FieldProjectionMapGet(null, mapInput.getIndex(), toJavaType((RexLiteral) key));
	}
}
