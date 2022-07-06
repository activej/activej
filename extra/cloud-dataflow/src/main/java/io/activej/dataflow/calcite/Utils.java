package io.activej.dataflow.calcite;

import io.activej.common.exception.ToDoException;
import io.activej.dataflow.calcite.function.ProjectionFunction;
import io.activej.dataflow.calcite.where.Operand;
import io.activej.dataflow.calcite.where.OperandRecordField;
import io.activej.dataflow.calcite.where.OperandScalar;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class Utils {
	public static <T> T toJavaType(RexLiteral literal) {
		SqlTypeName typeName = literal.getTypeName();

		return (T) switch (requireNonNull(typeName.getFamily())) {
			case CHARACTER -> literal.getValueAs(String.class);
			case NUMERIC, INTEGER -> literal.getValueAs(Integer.class);
			default -> throw new ToDoException();
		};
	}

	public static Operand toOperand(RexNode conditionNode) {
		if (conditionNode instanceof RexCall call) {
			switch (call.getKind()) {
				case CAST -> {
					return toOperand(call.getOperands().get(0));
				}
				case OTHER_FUNCTION -> {
					SqlOperator operator = call.getOperator();
					if (operator instanceof ProjectionFunction projectionFunction) {
						List<Operand> operands = call.getOperands()
								.stream()
								.map(Utils::toOperand)
								.collect(Collectors.toList());

						return projectionFunction.toOperand(operands);
					}
				}
			}
		} else if (conditionNode instanceof RexLiteral literal) {
			return new OperandScalar(toJavaType(literal));
		} else if (conditionNode instanceof RexInputRef inputRef) {
			return new OperandRecordField(inputRef.getIndex());
		}
		throw new IllegalArgumentException("Unknown node: " + conditionNode);
	}

}
