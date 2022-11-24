package io.activej.dataflow.calcite.inject;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.dataflow.calcite.inject.codec.CalciteCodecModule;
import io.activej.inject.Key;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.UniqueQualifierImpl;
import org.apache.calcite.avatica.remote.JsonService;

import java.util.Set;

public final class CalciteCommonModule extends AbstractModule {
	static {
		JsonService.MAPPER.registerModule(new JavaTimeModule());
	}

	@Override
	protected void configure() {
		install(new CalciteSerializersModule());
		install(CalciteCodecModule.create());

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
