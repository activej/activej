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

package io.activej.launchers.crdt;

import io.activej.common.reflection.TypeT;
import io.activej.config.Config;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.TimestampContainer;
import io.activej.eventloop.Eventloop;
import io.activej.fs.ActiveFs;
import io.activej.fs.LocalActiveFs;
import io.activej.http.AsyncHttpServer;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;

import java.util.concurrent.Executor;

import static io.activej.config.converter.ConfigConverters.ofExecutor;
import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class CrdtNodeExample extends CrdtNodeLauncher<String, TimestampContainer<Integer>> {
	@Inject
	AsyncHttpServer httpServer;

	@Override
	protected CrdtNodeLogicModule<String, TimestampContainer<Integer>> getBusinessLogicModule() {
		return new CrdtNodeLogicModule<String, TimestampContainer<Integer>>() {
			@Override
			protected void configure() {
				install(new CrdtHttpModule<String, TimestampContainer<Integer>>() {});
			}

			@Provides
			CrdtDescriptor<String, TimestampContainer<Integer>> descriptor() {
				return new CrdtDescriptor<>(
						TimestampContainer.createCrdtFunction(Integer::max),
						new CrdtDataSerializer<>(UTF8_SERIALIZER,
								TimestampContainer.createSerializer(INT_SERIALIZER)),
						TypeT.of(String.class),
						new TypeT<TimestampContainer<Integer>>() {});
			}

			@Provides
			Executor provideExecutor(Config config) {
				return config.get(ofExecutor(), "crdt.local.executor", newSingleThreadExecutor());
			}

			@Provides
			ActiveFs fs(Eventloop eventloop, Executor executor, Config config) {
				return LocalActiveFs.create(eventloop, executor, config.get(ofPath(), "crdt.local.path"));
			}
		};
	}

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			Config config() {
				return Config.create()
						.with("crdt.http.listenAddresses", "localhost:8080")
						.with("crdt.server.listenAddresses", "localhost:9090")
						.with("crdt.local.path", "/tmp/TESTS/crdt")
						.with("crdt.cluster.localPartitionId", "local")
						.with("crdt.cluster.partitions.noop", "localhost:9091")
						.with("crdt.cluster.server.listenAddresses", "localhost:9000")
						.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
						.overrideWith(Config.ofSystemProperties("config"));
			}
		};
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new CrdtNodeExample();
		launcher.launch(args);
	}
}
