package io.activej.dataflow.calcite.operand;

import io.activej.common.Utils;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.apache.calcite.rex.RexDynamicParam;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public final class Operand_MapGet extends FunctionOperand<Operand_MapGet> {
	private final Operand<?> mapOperand;
	private final Operand<?> keyOperand;

	public Operand_MapGet(Operand<?> mapOperand, Operand<?> keyOperand) {
		this.mapOperand = mapOperand;
		this.keyOperand = keyOperand;
	}

	@Override
	public <V> V getValue(Record record) {
		Map<?, V> map = mapOperand.getValue(record);
		Object key = keyOperand.getValue(record);

		if (map == null || key == null) return null;

		return map.get(convertKey(record, key));
	}

	@Override
	public Type getFieldType(RecordScheme original) {
		Type mapFieldType = mapOperand.getFieldType(original);
		return ((ParameterizedType) mapFieldType).getActualTypeArguments()[1];
	}

	@Override
	public String getFieldName(RecordScheme original) {
		String mapFieldName = mapOperand.getFieldName(original);
		String keyFieldName = keyOperand.getFieldName(original);
		return mapFieldName + ".get(" + keyFieldName + ")";
	}

	@Override
	public Operand_MapGet materialize(List<Object> params) {
		return new Operand_MapGet(
				mapOperand.materialize(params),
				keyOperand.materialize(params)
		);
	}

	@Override
	public List<RexDynamicParam> getParams() {
		return Utils.concat(mapOperand.getParams(), keyOperand.getParams());
	}

	public Operand<?> getMapOperand() {
		return mapOperand;
	}

	public Operand<?> getKeyOperand() {
		return keyOperand;
	}

	@Override
	public List<Operand<?>> getOperands() {
		return List.of(mapOperand, keyOperand);
	}

	private Object convertKey(Record record, Object key) {
		if (!(key instanceof BigDecimal bigDecimal)) return key;

		Type fieldType = mapOperand.getFieldType(record.getScheme());
		assert fieldType instanceof ParameterizedType;
		Type keyType = ((ParameterizedType) fieldType).getActualTypeArguments()[0];

		if (keyType == BigDecimal.class) return key;

		if (keyType == Byte.class) return bigDecimal.byteValue();
		if (keyType == Short.class) return bigDecimal.shortValue();
		if (keyType == Integer.class) return bigDecimal.intValue();
		if (keyType == Long.class) return bigDecimal.longValue();
		if (keyType == Float.class) return bigDecimal.floatValue();
		if (keyType == Double.class) return bigDecimal.doubleValue();

		throw new IllegalStateException("Cannot convert key to: " + keyType);
	}

	@Override
	public String toString() {
		return "OperandMapGet[" +
				"mapOperand=" + mapOperand + ", " +
				"keyOperand=" + keyOperand + ']';
	}
}
