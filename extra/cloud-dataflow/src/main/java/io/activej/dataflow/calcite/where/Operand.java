package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.SerializeClass;
import org.jetbrains.annotations.Nullable;

@SerializeClass(subclasses = {OperandRecordField.class, OperandScalar.class, OperandMapGet.class, OperandListGet.class})
public interface Operand {
	@Nullable <T> T getValue(Record record);
}
