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
import io.activej.inject.KeyPattern;
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
import java.util.List;
import java.util.Map;

public final class BinarySerializerModule extends AbstractModule {
	private static final Logger logger = LoggerFactory.getLogger(BinarySerializerModule.class);

	private BinarySerializerModule() {
	}

	public static Module create() {
		return new BinarySerializerModule();
	}

	@Override
	protected void configure() {
		Map<Type, Key<BinarySerializer<?>>> transientSerializers = new HashMap<>();

		transform(new KeyPattern<BinarySerializer<?>>() {}, (bindings, scope, key, binding) -> {
			Class<?> rawType = key.getTypeParameter(0).getRawType();

			if (binding.getType() == BindingType.TRANSIENT) {
				transientSerializers.put(rawType, key);
				return binding;
			}

			return binding
					.addDependencies(BinarySerializerLocator.class)
					.mapInstance(List.of(Key.of(BinarySerializerLocator.class)), (dependencies, serializer) -> {
						BinarySerializerLocator locator = (BinarySerializerLocator) dependencies[0];
						locator.serializers.putIfAbsent(rawType, serializer);
						return serializer;
					});
		});

		transform(BinarySerializerLocator.class,
				(bindings, scope, key, binding) -> binding.mapInstance(locator -> {
					locator.transientSerializers = transientSerializers;
					return locator;
				}));
	}

	@Provides
	BinarySerializerLocator serializerLocator(Injector injector, OptionalDependency<SerializerBuilder> optionalSerializerBuilder) {
		BinarySerializerLocator locator = new BinarySerializerLocator(injector);
		if (optionalSerializerBuilder.isPresent()) {
			locator.builder = optionalSerializerBuilder.get();
		}
		return locator;
	}

	@Provides
	<T> BinarySerializer<T> generator(BinarySerializerLocator locator, Key<T> key) {
		return locator.get(key.getType());
	}

	public static final class BinarySerializerLocator {
		private final Map<Type, BinarySerializer<?>> serializers = new HashMap<>();

		private Map<Type, Key<BinarySerializer<?>>> transientSerializers;
		private @Nullable SerializerBuilder builder = null;

		private final Injector injector;

		public BinarySerializerLocator(Injector injector) {
			this.injector = injector;
		}

		public <T> BinarySerializer<T> get(Class<T> cls) {
			return get(((Type) cls));
		}

		@SuppressWarnings({"unchecked"})
		public <T> BinarySerializer<T> get(Type type) {
			Key<BinarySerializer<?>> transientKey = transientSerializers.get(type);
			if (transientKey != null) {
				return (BinarySerializer<T>) injector.getInstance(transientKey);
			}
			return (BinarySerializer<T>) serializers.computeIfAbsent(type, aType -> {
				logger.trace("Creating serializer for {}", type);
				if (builder == null) {
					builder = SerializerBuilder.create();
				}
				return builder.build(aType);
			});
		}
	}
}
