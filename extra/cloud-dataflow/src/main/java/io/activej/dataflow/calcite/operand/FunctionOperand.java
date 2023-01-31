package io.activej.dataflow.calcite.operand;

import java.util.List;

public abstract class FunctionOperand<Self extends FunctionOperand<Self>> implements Operand<Self> {
	public abstract List<Operand<?>> getOperands();
}
