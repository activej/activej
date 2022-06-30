package io.activej.dataflow.calcite;

import io.activej.common.exception.ToDoException;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

import static java.util.Objects.requireNonNull;

public final class Utils {
	public static Object toJavaType(RexLiteral literal) {
		SqlTypeName typeName = literal.getTypeName();

		return switch (requireNonNull(typeName.getFamily())) {
			case CHARACTER -> literal.getValueAs(String.class);
			case NUMERIC, INTEGER -> literal.getValueAs(Integer.class);
			default -> throw new ToDoException();
		};
	}
}
