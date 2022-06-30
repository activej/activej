package io.activej.dataflow.calcite.function;

import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjection;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjectionListGet;
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

public final class ListGetFunction extends ProjectionFunction {

	public ListGetFunction() {
		super("LIST_GET", SqlKind.OTHER_FUNCTION, opBinding -> opBinding.getOperandType(0).getComponentType(),
				null, family(SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER), SqlFunctionCategory.USER_DEFINED_FUNCTION);
	}

	@Override
	public FieldProjection projectField(@Nullable String fieldName, List<RexNode> operands) {
		RexInputRef listInput = (RexInputRef) operands.get(0);
		RexNode key = operands.get(1);

		return new FieldProjectionListGet(null, listInput.getIndex(), (int) toJavaType((RexLiteral) key));
	}
}
