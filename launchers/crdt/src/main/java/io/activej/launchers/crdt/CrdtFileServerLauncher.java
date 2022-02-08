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
import io.activej.crdt.storage.local.CrdtStorageFs;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
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
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	ExecutorService executor(Config config) {
		return config.get(ofExecutor(), "executor");
	}

	@Provides
	LocalActiveFs localFsClient(Eventloop eventloop, ExecutorService executor, Config config) {
		return LocalActiveFs.create(eventloop, executor, config.get(ofPath(), "crdt.localPath"));
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
		CrdtServer<K, S> crdtServer(Eventloop eventloop, CrdtStorageFs<K, S> crdtClient, CrdtDescriptor<K, S> descriptor, Config config) {
			return CrdtServer.create(eventloop, crdtClient, descriptor.getSerializer())
					.withInitializer(ofAbstractServer(config.getChild("crdt.server")));
		}

		@Provides
		CrdtStorageFs<K, S> fsCrdtClient(Eventloop eventloop, LocalActiveFs localFsClient, CrdtDescriptor<K, S> descriptor) {
			return CrdtStorageFs.create(eventloop, localFsClient, descriptor.getSerializer(), descriptor.getCrdtFunction());
		}
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}
}
