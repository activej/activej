/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.dataflow.inject;

import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.BindingType;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public final class BinarySerializerModule extends AbstractModule {
	private static final Logger logger = LoggerFactory.getLogger(BinarySerializerModule.class);

	private final BinarySerializerLocator locator = new BinarySerializerLocator();

	private BinarySerializerModule() {
	}

	public static Module create() {
		return new BinarySerializerModule();
	}

	@Override
	protected void configure() {
		transform(BinarySerializer.class, (bindings, scope, key, binding) -> {
			Class<?> rawType = key.getTypeParameter(0).getRawType();

			if (binding.getType() == BindingType.TRANSIENT) {
				locator.transientSerializers.put(rawType, key);
				return binding;
			}

			return binding.mapInstance(serializer -> {
				locator.serializers.putIfAbsent(rawType, serializer);
				return serializer;
			});
		});
	}

	@Provides
	BinarySerializerLocator serializerLocator(Injector injector, OptionalDependency<SerializerBuilder> optionalSerializerBuilder) {
		locator.injector = injector;
		if (optionalSerializerBuilder.isPresent()) {
			locator.builder = optionalSerializerBuilder.get();
		}
		return locator;
	}

	@SuppressWarnings("rawtypes")
	public static final class BinarySerializerLocator {
		private final Map<Type, Key<BinarySerializer>> transientSerializers = new HashMap<>();
		private final Map<Type, BinarySerializer<?>> serializers = new HashMap<>();
		private @Nullable SerializerBuilder builder = null;

		private Injector injector;

		public <T> BinarySerializer<T> get(Class<T> cls) {
			return get(((Type) cls));
		}

		@SuppressWarnings("unchecked")
		public <T> BinarySerializer<T> get(Type type) {
			Key<BinarySerializer> transientKey = transientSerializers.get(type);
			if (transientKey != null) {
				return (BinarySerializer<T>) injector.getInstance(transientKey);
			}
			return (BinarySerializer<T>) serializers.computeIfAbsent(type, aType -> {
				logger.info("Creating serializer for {}", type);
				if (builder == null) {
					builder = SerializerBuilder.create();
				}
				return builder.build(aType);
			});
		}
	}
}
