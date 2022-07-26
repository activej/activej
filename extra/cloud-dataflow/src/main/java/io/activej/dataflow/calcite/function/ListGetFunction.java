package io.activej.dataflow.calcite.function;

import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjection;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjectionListGet;
import io.activej.dataflow.calcite.Value;
import io.activej.dataflow.calcite.where.Operand;
import io.activej.dataflow.calcite.where.OperandListGet;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static io.activej.common.Checks.checkArgument;
import static io.activej.dataflow.calcite.Utils.toJavaType;
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
	public FieldProjection projectField(@Nullable String fieldName, List<RexNode> operands) {
		RexInputRef listInput = (RexInputRef) operands.get(0);
		RexNode indexNode = operands.get(1);

		Value index = switch (indexNode.getKind()) {
			case LITERAL -> Value.materializedValue(toJavaType((RexLiteral) indexNode));
			case DYNAMIC_PARAM -> Value.unmaterializedValue((RexDynamicParam) indexNode);
			default -> throw new IllegalArgumentException("Unsupported index type");
		};

		return new FieldProjectionListGet(null, listInput.getIndex(), index);
	}

	@Override
	public Operand toOperand(List<Operand> operands) {
		checkArgument(operands.size() == 2);

		Operand listOperand = operands.get(0);
		Operand indexOperand = operands.get(1);
		return new OperandListGet(listOperand, indexOperand);
	}
}
