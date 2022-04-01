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

import io.activej.codegen.DefiningClassLoader;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
			return binding.mapInstance(serializer -> {
				locator.serializers.putIfAbsent(rawType, serializer);
				return serializer;
			});
		});
	}

	@Provides
	BinarySerializerLocator serializerLocator() {
		return locator;
	}

	public static final class BinarySerializerLocator {
		private final Map<Class<?>, BinarySerializer<?>> serializers = new HashMap<>();
		private @Nullable SerializerBuilder builder = null;

		@SuppressWarnings("unchecked")
		public <T> BinarySerializer<T> get(Class<T> cls) {
			return (BinarySerializer<T>) serializers.computeIfAbsent(cls, type -> {
				logger.info("Creating serializer for {}", type);
				if (builder == null) {
					builder = SerializerBuilder.create(DefiningClassLoader.create(Thread.currentThread().getContextClassLoader()));
				}
				return builder.build(type);
			});
		}
	}
}
