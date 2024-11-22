package io.activej.dataflow.calcite.operand.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.common.collection.CollectionUtils;
import io.activej.dataflow.calcite.Param;
import io.activej.dataflow.calcite.operand.FunctionOperand;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@ExposedInternals
public final class MapGet extends FunctionOperand<MapGet> {
	public final Operand<?> mapOperand;
	public final Operand<?> keyOperand;

	public MapGet(Operand<?> mapOperand, Operand<?> keyOperand) {
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
	public MapGet materialize(List<Object> params) {
		return new MapGet(
			mapOperand.materialize(params),
			keyOperand.materialize(params)
		);
	}

	@Override
	public List<Param> getParams() {
		return CollectionUtils.concat(mapOperand.getParams(), keyOperand.getParams());
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
		return
			"MapGet[" +
			"mapOperand=" + mapOperand + ", " +
			"keyOperand=" + keyOperand + ']';
	}
}
