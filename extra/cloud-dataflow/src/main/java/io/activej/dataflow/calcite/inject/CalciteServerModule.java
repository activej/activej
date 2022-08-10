package io.activej.dataflow.calcite.inject;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.dataflow.inject.DatasetId;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.DIException;
import io.activej.inject.module.AbstractModule;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.util.List;
import java.util.Map;

import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;

public final class CalciteServerModule extends AbstractModule {

	public static final String CALCITE_SINGLE_DUMMY_DATASET = "_calcite_single_dummy";

	@Override
	protected void configure() {
		transform(DataflowSchema.class, (bindings, scope, key, binding) -> binding
				.addDependencies(Injector.class)
				.mapInstance(List.of(Key.of(Injector.class)), (deps, schema) -> {
					Injector injector = (Injector) deps[0];
					Map<String, DataflowTable<?>> tableMap = schema.getDataflowTableMap();

					for (Map.Entry<String, DataflowTable<?>> entry : tableMap.entrySet()) {
						String id = entry.getKey().toLowerCase();
						Key<Object> datasetKey = datasetId(id);
						if (injector.getBinding(datasetKey) == null) {
							throw new DIException("No binding for a dataset of " + entry.getValue().getType().getName() + " with ID '" + id + '\'');
						}
					}

					return schema;
				}));
	}

	@Provides
	@DatasetId(CALCITE_SINGLE_DUMMY_DATASET)
	List<Record> calciteSingleDummy(DefiningClassLoader classLoader) {
		RecordScheme emptyScheme = RecordScheme.create(classLoader).build();
		return List.of(emptyScheme.record());
	}
}
