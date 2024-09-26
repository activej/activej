package io.activej.dataflow.calcite.optimizer;

import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;

abstract class LimitedConfig implements RelRule.Config {
	@Override
	public final RelRule.Config withRelBuilderFactory(RelBuilderFactory factory) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final String description() {
		return null;
	}

	@Override
	public final RelRule.Config withDescription(@org.checkerframework.checker.nullness.qual.Nullable String description) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final RelRule.Config withOperandSupplier(RelRule.OperandTransform transform) {
		throw new UnsupportedOperationException();
	}
}
