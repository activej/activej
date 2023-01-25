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

import io.activej.config.Config;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.util.BinarySerializer_CrdtData;
import io.activej.fs.IFileSystem;
import io.activej.fs.FileSystem;
import io.activej.http.HttpServer;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.reactor.Reactor;

import java.util.concurrent.Executor;

import static io.activej.config.converter.ConfigConverters.ofExecutor;
import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class CrdtNodeExample extends CrdtNodeLauncher<String, Integer> {
	@Inject
	HttpServer httpServer;

	@Override
	protected CrdtNodeLogicModule<String, Integer> getBusinessLogicModule() {
		return new CrdtNodeLogicModule<String, Integer>() {
			@Override
			protected void configure() {
				install(new CrdtHttpModule<String, Integer>() {});
			}

			@Provides
			CrdtDescriptor<String, Integer> descriptor() {
				return new CrdtDescriptor<>(
						CrdtFunction.ignoringTimestamp(Integer::max),
						new BinarySerializer_CrdtData<>(UTF8_SERIALIZER, INT_SERIALIZER),
						String.class,
						Integer.class);
			}

			@Provides
			Executor provideExecutor(Config config) {
				return config.get(ofExecutor(), "crdt.local.executor", newSingleThreadExecutor());
			}

			@Provides
			IFileSystem fileSystem(Reactor reactor, Executor executor, Config config) {
				return FileSystem.create(reactor, executor, config.get(ofPath(), "crdt.local.path"));
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
