package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import org.jetbrains.annotations.Nullable;

public interface Operand {
	@Nullable <T> T getValue(Record record);
}
