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

package io.activej.launchers.fs;

import io.activej.fs.IFileSystem;
import io.activej.fs.cluster.ClusterRepartitionController;
import io.activej.http.AsyncServlet;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launchers.fs.gui.FileSystemGuiModule;
import io.activej.launchers.fs.gui.FileSystemGuiServlet;

public class ClusterTcpServerGuiLauncher extends ClusterTcpServerLauncher {
	@Override
	protected Module getBusinessLogicModule() {
		return FileSystemGuiModule.create();
	}

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			AsyncServlet guiServlet(IFileSystem fs, ClusterRepartitionController controller) {
				return FileSystemGuiServlet.create(controller.getReactor(), fs,
					"Cluster server [" +
					controller.getLocalPartitionId() + ']');
			}
		};
	}

	public static void main(String[] args) throws Exception {
		new ClusterTcpServerGuiLauncher().launch(args);
	}
}
