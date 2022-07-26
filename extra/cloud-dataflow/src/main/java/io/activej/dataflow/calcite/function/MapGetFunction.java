package io.activej.dataflow.calcite.function;

import io.activej.dataflow.calcite.RecordProjectionFn;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjectionMapGet;
import io.activej.dataflow.calcite.Value;
import io.activej.dataflow.calcite.where.Operand;
import io.activej.dataflow.calcite.where.OperandMapGet;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static io.activej.common.Checks.checkArgument;
import static io.activej.dataflow.calcite.Utils.toJavaType;
import static org.apache.calcite.sql.type.OperandTypes.family;

public final class MapGetFunction extends ProjectionFunction {
	public MapGetFunction() {
		super("MAP_GET", SqlKind.OTHER_FUNCTION, opBinding -> opBinding.getOperandType(0).getValueType(),
				InferTypes.ANY_NULLABLE, family(SqlTypeFamily.MAP, SqlTypeFamily.ANY), SqlFunctionCategory.USER_DEFINED_FUNCTION);
	}

	@Override
	public RecordProjectionFn.FieldProjection projectField(@Nullable String fieldName, List<RexNode> operands) {
		RexInputRef mapInput = (RexInputRef) operands.get(0);
		RexNode key = operands.get(1);

		Value keyValue = switch (key.getKind()) {
			case LITERAL -> Value.materializedValue(toJavaType((RexLiteral) key));
			case DYNAMIC_PARAM -> Value.unmaterializedValue(((RexDynamicParam) key));
			default -> throw new IllegalArgumentException("Unsupported key type");
		};

		return new FieldProjectionMapGet(null, mapInput.getIndex(), keyValue);
	}

	@Override
	public Operand toOperand(List<Operand> operands) {
		checkArgument(operands.size() == 2);

		Operand mapOperand = operands.get(0);
		Operand keyOperand = operands.get(1);
		return new OperandMapGet<>(mapOperand, keyOperand);
	}
}
