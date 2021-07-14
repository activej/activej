package io.activej.dataflow.inject;

import io.activej.inject.Key;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;

import java.util.HashMap;
import java.util.Map;

public final class DatasetIdModule extends AbstractModule {

	private DatasetIdModule() {
	}

	public static Module create() {
		return new DatasetIdModule();
	}

	@Override
	protected void configure() {
		DatasetIds datasetIds = new DatasetIds();
		bind(DatasetIds.class).toInstance(datasetIds);
		transform(Object.class, (bindings, scope, key, binding) -> {
			if (key.getQualifier() instanceof DatasetId) {
				String id = ((DatasetId) key.getQualifier()).value();
				Key<?> previousKey = datasetIds.keys.put(id, key);
				if (previousKey != null && !previousKey.equals(key)) {
					throw new IllegalStateException("More than one items provided for dataset id '" + id + "'");
				}
			}
			return binding;
		});
	}

	public static final class DatasetIds {
		final Map<String, Key<?>> keys = new HashMap<>();

		@SuppressWarnings("unchecked")
		public <T> Key<T> getKeyForId(String id) {
			Key<?> key = keys.get(id);
			if (key == null) {
				throw new IllegalStateException("No key for dataset id '" + id + "'");
			}
			return (Key<T>) key;
		}
	}
}
