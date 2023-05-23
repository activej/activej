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
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.launchers.crdt.CrdtNodeLogicModule.Cluster;
import io.activej.service.ServiceGraphModule;
import io.activej.trigger.TriggersModule;

import static io.activej.config.Config.ofClassPathProperties;
import static io.activej.config.Config.ofSystemProperties;
import static io.activej.inject.module.Modules.combine;

public abstract class CrdtNodeLauncher<K extends Comparable<K>, S> extends Launcher {
	public static final String PROPERTIES_FILE = "crdt-node.properties";

	@Inject
	@Cluster
	CrdtServer<K, S> clusterServer;

	@Inject
	CrdtServer<K, S> crdtServer;

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
			ConfigModule.builder()
				.withEffectiveConfigLogger()
				.build(),
			getBusinessLogicModule());
	}

	protected abstract CrdtNodeLogicModule<K, S> getBusinessLogicModule();

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}
}
