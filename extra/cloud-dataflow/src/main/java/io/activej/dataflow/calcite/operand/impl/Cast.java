package io.activej.dataflow.calcite.operand.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.dataflow.calcite.Param;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.lang.reflect.Type;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import static io.activej.dataflow.calcite.utils.Utils.DATE_TIME_FORMATTER;

@ExposedInternals
public final class Cast implements Operand<Cast> {
	public final Operand<?> valueOperand;
	public final int type;

	public Cast(Operand<?> valueOperand, int type) {
		this.valueOperand = valueOperand;
		this.type = type;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getValue(Record record) {
		Object value = valueOperand.getValue(record);
		if (value == null) return null;

		return (T) switch (type) {
			case Types.DATE -> LocalDate.parse(((String) value));
			case Types.TIMESTAMP -> LocalDateTime.parse((String) value, DATE_TIME_FORMATTER);
			case Types.TIME -> LocalTime.parse(((String) value));
			case Types.INTEGER -> ((Number) value).intValue();
			case Types.BIGINT -> ((Number) value).longValue();
			case Types.FLOAT -> ((Number) value).floatValue();
			case Types.DOUBLE -> ((Number) value).doubleValue();
			default -> value;
		};
	}

	@Override
	public Type getFieldType(RecordScheme original) {
		return switch (type) {
			case Types.DATE -> LocalDate.class;
			case Types.TIMESTAMP -> LocalDateTime.class;
			case Types.TIME -> LocalTime.class;
			case Types.INTEGER -> Integer.class;
			case Types.BIGINT -> Long.class;
			case Types.FLOAT -> Float.class;
			case Types.DOUBLE -> Double.class;
			default -> valueOperand.getFieldType(original);
		};
	}

	@Override
	public String getFieldName(RecordScheme original) {
		return valueOperand.getFieldName(original);
	}

	@Override
	public Cast materialize(List<Object> params) {
		return new Cast(valueOperand.materialize(params), type);
	}

	@Override
	public List<Param> getParams() {
		return valueOperand.getParams();
	}

	@Override
	public String toString() {
		return
			"Cast[" +
			"valueOperand=" + valueOperand +
			", type='" + type + '\'' +
			']';
	}
}
