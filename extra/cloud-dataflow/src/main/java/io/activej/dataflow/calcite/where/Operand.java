package io.activej.dataflow.calcite.where;

import io.activej.record.Record;
import io.activej.serializer.annotations.SerializeClass;

@SerializeClass(subclasses = {OperandField.class, OperandScalar.class})
public interface Operand<T> {
	T getValue(Record record);
}
