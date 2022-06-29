package io.activej.dataflow.calcite.inject;

import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.binding.DIException;
import io.activej.inject.module.AbstractModule;

import java.util.List;
import java.util.Map;

import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;

public final class CalciteServerModule extends AbstractModule {

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
}
