package io.activej.dataflow.inject;

import io.activej.common.function.SupplierEx;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.datastream.StreamSupplier;
import io.activej.inject.Key;
import io.activej.inject.binding.BindingType;
import io.activej.inject.module.AbstractModule;
import io.activej.promise.Promise;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class DatasetIdModule extends AbstractModule {

	private DatasetIdModule() {
	}

	public static DatasetIdModule create() {
		return new DatasetIdModule();
	}

	@Override
	protected void configure() {
		DatasetIds datasetIds = new DatasetIds();
		bind(DatasetIds.class).toInstance(datasetIds);
		transform(Object.class, (bindings, scope, key, binding) -> {
			Object qualifier = key.getQualifier();
			if (qualifier instanceof DatasetId datasetId) {
				String id = datasetId.value();
				Class<?> rawType = key.getRawType();

				if (binding.getType() != BindingType.TRANSIENT &&
						(rawType == Iterator.class ||
								rawType == Supplier.class ||
								rawType == SupplierEx.class ||
								rawType == Stream.class ||
								rawType == StreamSupplier.class ||
								rawType == ChannelSupplier.class ||
								rawType == Promise.class)) {
					throw new IllegalStateException("Provided dataset '" + datasetId.value() + "' is a single use dataset. " +
							"Maybe mark it as @Transient?");
				}

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
