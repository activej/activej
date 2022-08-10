package io.activej.dataflow.calcite.utils;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.exception.ToDoException;
import io.activej.dataflow.calcite.Value;
import io.activej.dataflow.calcite.function.ProjectionFunction;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.operand.OperandFieldAccess;
import io.activej.dataflow.calcite.operand.OperandRecordField;
import io.activej.dataflow.calcite.operand.OperandScalar;
import org.apache.calcite.rex.*;
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

	public static Operand<?> toOperand(RexNode node, DefiningClassLoader classLoader) {
		if (node instanceof RexDynamicParam dynamicParam) {
			return new OperandScalar(Value.unmaterializedValue(dynamicParam));
		} else if (node instanceof RexCall call) {
			switch (call.getKind()) {
				case CAST -> {
					return toOperand(call.getOperands().get(0), classLoader);
				}
				case OTHER_FUNCTION -> {
					SqlOperator operator = call.getOperator();
					if (operator instanceof ProjectionFunction projectionFunction) {
						List<Operand<?>> operands = call.getOperands()
								.stream()
								.map(operand -> toOperand(operand, classLoader))
								.collect(Collectors.toList());

						return projectionFunction.toOperand(operands);
					}
				}
			}
		} else if (node instanceof RexLiteral literal) {
			Value value = Value.materializedValue(literal);
			return new OperandScalar(value);
		} else if (node instanceof RexInputRef inputRef) {
			return new OperandRecordField(inputRef.getIndex());
		} else if (node instanceof RexFieldAccess fieldAccess) {
			Operand<?> objectOperand = toOperand(fieldAccess.getReferenceExpr(), classLoader);
			return new OperandFieldAccess(objectOperand, fieldAccess.getField().getName(), classLoader);
		}
		throw new IllegalArgumentException("Unknown node: " + node);
	}

	public static int compareToUnknown(Comparable<Object> comparable1, Comparable<Object> comparable2) {
		if (comparable1.getClass() != comparable2.getClass() &&
				comparable1 instanceof Number number1 && comparable2 instanceof Number number2) {
			return Double.compare(number1.doubleValue(), number2.doubleValue());
		}

		return comparable1.compareTo(comparable2);
	}

	public static boolean equalsUnknown(Object object1, Object object2) {
		if (object1.getClass() != object2.getClass() &&
				object1 instanceof Number number1 && object2 instanceof Number number2) {
			return Double.compare(number1.doubleValue(), number2.doubleValue()) == 0;
		}

		return object1.equals(object2);
	}
}
