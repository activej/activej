package io.activej.dataflow.calcite.inject;

import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.inject.Key;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.UniqueQualifierImpl;

import java.util.Set;

public final class CalciteCommonModule extends AbstractModule {
	@Override
	protected void configure() {
		install(new SerializersModule());

		transform(DataflowTable.class, (bindings, scope, key, binding) -> {
			if (key.getQualifier() instanceof UniqueQualifierImpl) {
				return binding;
			}
			throw new IllegalStateException("Dataflow tables should be provided into set (@ProvidesIntoSet): " + binding.getLocation());
		});

		bind(DataflowSchema.class).to(tables -> DataflowSchema.create().withTables(tables),
				new Key<Set<DataflowTable>>() {});
	}
}
