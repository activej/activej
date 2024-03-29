package io.activej.dataflow.calcite.inject;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.table.AbstractDataflowTable;
import io.activej.dataflow.inject.DatasetId;
import io.activej.dataflow.inject.DatasetIdModule.DatasetIds;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.util.List;
import java.util.Map;

public final class CalciteServerModule extends AbstractModule {

	public static final String CALCITE_SINGLE_DUMMY_DATASET = "_calcite_single_dummy";

	private CalciteServerModule() {
	}

	public static CalciteServerModule create() {
		return new CalciteServerModule();
	}

	@Override
	protected void configure() {
		install(new CalciteCommonModule());

		transform(DataflowSchema.class, (bindings, scope, key, binding) -> binding
			.addDependencies(DatasetIds.class)
			.mapInstance(List.of(Key.of(DatasetIds.class)), (deps, schema) -> {
				DatasetIds datasetIds = (DatasetIds) deps[0];
				Map<String, AbstractDataflowTable<?>> tableMap = schema.getDataflowTableMap();

				for (Map.Entry<String, AbstractDataflowTable<?>> entry : tableMap.entrySet()) {
					String id = entry.getKey();
					datasetIds.getKeyForId(id); // throws exception if no key
				}

				return schema;
			}));
	}

	@Provides
	@DatasetId(CALCITE_SINGLE_DUMMY_DATASET)
	List<Record> calciteSingleDummy(DefiningClassLoader classLoader) {
		RecordScheme emptyScheme = RecordScheme.builder(classLoader).withComparatorFields().build();
		return List.of(emptyScheme.record());
	}
}
