package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import org.apache.calcite.rex.RexDynamicParam;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public interface Operand {
	@Nullable <T> T getValue(Record record);

	Operand materialize(List<Object> params);

	List<RexDynamicParam> getParams();
}
