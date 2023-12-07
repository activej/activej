package io.activej.dataflow.calcite.operand;

import io.activej.dataflow.calcite.Param;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.List;

public interface Operand<Self extends Operand<Self>> {
	@Nullable <T> T getValue(Record record);

	Type getFieldType(RecordScheme original);

	String getFieldName(RecordScheme original);

	Self materialize(List<Object> params);

	List<Param> getParams();
}
