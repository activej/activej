package io.activej.dataflow.calcite.operand;

import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.apache.calcite.rex.RexDynamicParam;

import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

public final class OperandCast implements Operand<OperandCast> {
	private final Operand<?> valueOperand;
	private final int type;

	public OperandCast(Operand<?> valueOperand, int type) {
		this.valueOperand = valueOperand;
		this.type = type;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getValue(Record record) {
		Object value = valueOperand.getValue(record);
		if (value == null) return null;

		return (T) switch (type) {
			case Types.DATE -> Date.valueOf(((String) value));
			case Types.TIMESTAMP -> Timestamp.valueOf(((String) value));
			case Types.TIME -> Time.valueOf(((String) value));
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
			case Types.DATE -> Date.class;
			case Types.TIMESTAMP -> Timestamp.class;
			case Types.TIME -> Time.class;
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
	public OperandCast materialize(List<Object> params) {
		return new OperandCast(valueOperand.materialize(params), type);
	}

	@Override
	public List<RexDynamicParam> getParams() {
		return valueOperand.getParams();
	}

	public Operand<?> getValueOperand() {
		return valueOperand;
	}

	public int getType() {
		return type;
	}

	@Override
	public String toString() {
		return "OperandCast[" +
				"valueOperand=" + valueOperand +
				", type='" + type + '\'' +
				']';
	}
}
