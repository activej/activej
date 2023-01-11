package io.activej.dataflow.calcite.operand;

import java.util.List;

public abstract class Operand_Function<Self extends Operand_Function<Self>> implements Operand<Self> {
	public abstract List<Operand<?>> getOperands();
}
