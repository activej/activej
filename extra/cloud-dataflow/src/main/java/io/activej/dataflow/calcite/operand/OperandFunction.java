package io.activej.dataflow.calcite.operand;

import java.util.List;

public abstract class OperandFunction<Self extends OperandFunction<Self>> implements Operand<Self> {
	public abstract List<Operand<?>> getOperands();
}
