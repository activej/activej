package io.activej.dataflow.calcite;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.activej.dataflow.calcite.utils.Utils;
import io.activej.record.Record;
import io.activej.types.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.activej.common.Checks.checkState;

@SuppressWarnings("ConstantConditions")
public class Value {
	private final Type type;
	private final @Nullable RexDynamicParam dynamicParam;
	private final @Nullable Object value;

	private Value(Type type, @Nullable Object value, @Nullable RexDynamicParam dynamicParam) {
		this.type = type;
		this.value = value;
		this.dynamicParam = dynamicParam;
	}

	public static Value materializedValue(Type type, @Nullable Object value) {
		return new Value(type, value, null);
	}

	public static <T> Value materializedValue(Class<T> type, @Nullable T value) {
		return new Value(type, value, null);
	}

	public static Value materializedValue(RexLiteral literal) {
		Type type = getJavaType(literal.getType());
		return new Value(type, Utils.toJavaType(literal), null);
	}

	public static Value unmaterializedValue(RexDynamicParam dynamicParam) {
		Type type = getJavaType(dynamicParam.getType());
		return new Value(type, null, dynamicParam);
	}

	public Value materialize(List<Object> params) {
		if (isMaterialized()) return this;

		return Value.materializedValue(type, params.get(dynamicParam.getIndex()));
	}

	public @Nullable Object getValue() {
		checkState(isMaterialized());

		return value;
	}

	public Type getType() {
		return type;
	}

	@JsonIgnore
	public RexDynamicParam getDynamicParam() {
		checkState(!isMaterialized());

		return dynamicParam;
	}

	public boolean isMaterialized() {
		return dynamicParam == null;
	}

	@Override
	public String toString() {
		return isMaterialized() ? Objects.toString(value) : "<UNKNOWN(" + dynamicParam.getIndex() + ")>";
	}

	private static Type getJavaType(RelDataType dataType) {
		if (dataType instanceof RelDataTypeFactoryImpl.JavaType javaType) {
			return javaType.getJavaClass();
		}
		if (dataType instanceof MapSqlType mapSqlType) {
			return Types.parameterizedType(Map.class, getJavaType(mapSqlType.getKeyType()), getJavaType(mapSqlType.getValueType()));
		}
		if (dataType instanceof ArraySqlType arraySqlType) {
			return Types.parameterizedType(List.class, getJavaType(arraySqlType.getComponentType()));
		}
		if (dataType instanceof RelRecordType) {
			return Record.class;
		}
		return switch (dataType.getSqlTypeName()) {
			case BOOLEAN -> boolean.class;
			case TINYINT, SMALLINT, INTEGER -> int.class;
			case BIGINT -> long.class;
			case DECIMAL, REAL, DOUBLE -> double.class;
			case FLOAT -> float.class;
			case CHAR, VARCHAR -> String.class;
			case BINARY, VARBINARY -> byte[].class;
			case NULL, UNKNOWN, ANY -> Object.class;
			default -> throw new AssertionError();
		};
	}
}
