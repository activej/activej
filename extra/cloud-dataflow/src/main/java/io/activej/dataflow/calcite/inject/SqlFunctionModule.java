package io.activej.dataflow.calcite.inject;

import io.activej.dataflow.calcite.function.ListGetFunction;
import io.activej.dataflow.calcite.function.MapGetFunction;
import io.activej.inject.annotation.ProvidesIntoSet;
import io.activej.inject.module.AbstractModule;
import org.apache.calcite.sql.SqlOperator;

public final class SqlFunctionModule extends AbstractModule {
	@ProvidesIntoSet
	SqlOperator mapGet() {
		return new MapGetFunction();
	}

	@ProvidesIntoSet
	SqlOperator listGet() {
		return new ListGetFunction();
	}
}
