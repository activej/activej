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
import io.activej.config.ConfigModule;
import io.activej.crdt.CrdtServer;
import io.activej.crdt.storage.local.FsCrdtStorage;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalFs;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;
import io.activej.trigger.TriggersModule;

import java.util.concurrent.ExecutorService;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.config.converter.ConfigConverters.ofExecutor;
import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;

public abstract class CrdtFileServerLauncher<K extends Comparable<K>, S> extends Launcher {
	public static final String PROPERTIES_FILE = "crdt-file-server.properties";

	@Inject
	CrdtServer<K, S> crdtServer;

	@Provides
	NioReactor reactor() {
		return Eventloop.create();
	}

	@Provides
	ExecutorService executor(Config config) {
		return config.get(ofExecutor(), "executor");
	}

	@Provides
	LocalFs localFsClient(Reactor reactor, ExecutorService executor, Config config) {
		return LocalFs.create(reactor, executor, config.get(ofPath(), "crdt.localPath"));
	}

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(ofSystemProperties("config"));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				TriggersModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger(),
				getBusinessLogicModule());
	}

	protected abstract CrdtFileServerLogicModule<K, S> getBusinessLogicModule();

	public abstract static class CrdtFileServerLogicModule<K extends Comparable<K>, S> extends AbstractModule {
		@Provides
		CrdtServer<K, S> crdtServer(NioReactor reactor, FsCrdtStorage<K, S> crdtClient, CrdtDescriptor<K, S> descriptor, Config config) {
			return CrdtServer.create(reactor, crdtClient, descriptor.serializer())
					.withInitializer(ofAbstractServer(config.getChild("crdt.server")));
		}

		@Provides
		FsCrdtStorage<K, S> fsCrdtClient(Reactor reactor, LocalFs localFsClient, CrdtDescriptor<K, S> descriptor) {
			return FsCrdtStorage.create(reactor, localFsClient, descriptor.serializer(), descriptor.crdtFunction());
		}
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}
}
