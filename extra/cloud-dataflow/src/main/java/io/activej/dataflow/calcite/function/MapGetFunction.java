package io.activej.dataflow.calcite.function;

import io.activej.dataflow.calcite.RecordProjectionFn;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjectionMapGet;
import io.activej.dataflow.calcite.where.Operand;
import io.activej.dataflow.calcite.where.OperandMapGet;
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
	public static final Object UNKNOWN_KEY = new Object();

	public MapGetFunction() {
		super("MAP_GET", SqlKind.OTHER_FUNCTION, opBinding -> opBinding.getOperandType(0).getValueType(),
				InferTypes.ANY_NULLABLE, family(SqlTypeFamily.MAP, SqlTypeFamily.ANY), SqlFunctionCategory.USER_DEFINED_FUNCTION);
	}

	@Override
	public RecordProjectionFn.FieldProjection projectField(@Nullable String fieldName, List<RexNode> operands) {
		RexInputRef mapInput = (RexInputRef) operands.get(0);
		RexNode key = operands.get(1);

		Object keyObject = switch (key.getKind()) {
			case LITERAL -> toJavaType((RexLiteral) key);
			case DYNAMIC_PARAM -> UNKNOWN_KEY;
			default -> throw new IllegalArgumentException("Unsupported key type");
		};

		return new FieldProjectionMapGet(null, mapInput.getIndex(), keyObject);
	}

	@Override
	public Operand toOperand(List<Operand> operands) {
		checkArgument(operands.size() == 2);

		Operand mapOperand = operands.get(0);
		Operand keyOperand = operands.get(1);
		return new OperandMapGet<>(mapOperand, keyOperand);
	}
}
