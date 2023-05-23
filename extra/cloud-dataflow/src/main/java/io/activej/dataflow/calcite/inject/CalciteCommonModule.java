package io.activej.dataflow.calcite.inject;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.inject.codec.CalciteCodecModule;
import io.activej.dataflow.calcite.table.AbstractDataflowTable;
import io.activej.dataflow.calcite.table.DataflowPartitionedTable;
import io.activej.dataflow.calcite.table.DataflowTable;
import io.activej.inject.Key;
import io.activej.inject.KeyPattern;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.UniqueQualifierImpl;
import org.apache.calcite.avatica.remote.JsonService;

import java.util.HashSet;
import java.util.Set;

import static io.activej.common.Checks.checkState;

public final class CalciteCommonModule extends AbstractModule {
	static {
		JsonService.MAPPER.registerModule(new JavaTimeModule());
	}

	@Override
	protected void configure() {
		install(new CalciteSerializersModule());
		install(CalciteCodecModule.create());

		transform(new KeyPattern<AbstractDataflowTable<?>>() {}, (bindings, scope, key, binding) -> {
			if (key.getQualifier() instanceof UniqueQualifierImpl) {
				return binding;
			}
			throw new IllegalStateException("Dataflow tables should be provided into set (@ProvidesIntoSet): " + binding.getLocation());
		});

		bind(DataflowSchema.class).to((optionalAbstractTables, optionalTables, optionalPartitionalTables) -> {
				Set<AbstractDataflowTable<?>> tables = new HashSet<>();

				if (optionalAbstractTables.isPresent()) tables.addAll(optionalAbstractTables.get());
				if (optionalTables.isPresent()) tables.addAll(optionalTables.get());
				if (optionalPartitionalTables.isPresent()) tables.addAll(optionalPartitionalTables.get());

				checkState(!tables.isEmpty(),
					"Cannot create schema with no tables, provide tables into set (@ProvidesIntoSet)");

				return DataflowSchema.builder().withTables(tables).build();
			},
			new Key<OptionalDependency<Set<AbstractDataflowTable<?>>>>() {},
			new Key<OptionalDependency<Set<DataflowTable<?>>>>() {},
			new Key<OptionalDependency<Set<DataflowPartitionedTable<?>>>>() {}
		);
	}
}
