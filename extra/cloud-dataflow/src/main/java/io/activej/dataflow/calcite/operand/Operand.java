package io.activej.dataflow.calcite.operand;

import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.apache.calcite.rex.RexDynamicParam;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.List;

public interface Operand<Self extends Operand<Self>> {
	@Nullable <T> T getValue(Record record);

	Type getFieldType(RecordScheme original);

	String getFieldName(RecordScheme original);

	Self materialize(List<Object> params);

	List<RexDynamicParam> getParams();
}
