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

import io.activej.common.exception.MalformedDataException;
import io.activej.config.Config;
import io.activej.crdt.CrdtData;
import io.activej.crdt.storage.local.CrdtStorageMap;
import io.activej.eventloop.Eventloop;
import io.activej.http.*;
import io.activej.http.loader.StaticLoader;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.promise.Promise;
import io.activej.types.TypeT;

import java.util.concurrent.Executor;

import static io.activej.crdt.util.Utils.fromJson;
import static io.activej.crdt.util.Utils.toJson;
import static io.activej.http.HttpMethod.*;
import static io.activej.launchers.initializers.Initializers.ofHttpServer;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class CrdtHttpModule<K extends Comparable<K>, S> extends AbstractModule {
	private final TypeT<CrdtData<K, S>> crdtDataManifest = new TypeT<CrdtData<K, S>>() {};

	@Provides
	AsyncHttpServer server(Eventloop eventloop, AsyncServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.withInitializer(ofHttpServer(config.getChild("crdt.http")));
	}

	@Provides
	StaticLoader loader(Executor executor) {
		return StaticLoader.ofClassPath(executor, "/");
	}

	@Provides
	AsyncServlet servlet(
			CrdtDescriptor<K, S> descriptor,
			CrdtStorageMap<K, S> client,
			OptionalDependency<BackupService<K, S>> backupServiceOpt
	) {
		RoutingServlet servlet = RoutingServlet.create()
				.map(POST, "/", request -> request.loadBody()
						.map(body -> {
							try {
								K key = fromJson(descriptor.getKeyManifest(), body);
								S state = client.get(key);
								if (state != null) {
									return HttpResponse.ok200()
											.withBody(toJson(descriptor.getStateManifest(), state));
								}
								return HttpResponse.ofCode(404)
										.withBody(("Key '" + key + "' not found").getBytes(UTF_8));
							} catch (MalformedDataException e) {
								throw HttpError.ofCode(400, e);
							}
						}))
				.map(PUT, "/", request -> request.loadBody()
						.map(body -> {
							try {
								client.put(fromJson(crdtDataManifest.getType(), body));
								return HttpResponse.ok200();
							} catch (MalformedDataException e) {
								throw HttpError.ofCode(400, e);
							}
						}))
				.map(DELETE, "/", request -> request.loadBody()
						.map(body -> {
							try {
								K key = fromJson(descriptor.getKeyManifest(), body);
								if (client.remove(key)) {
									return HttpResponse.ok200();
								}
								return HttpResponse.ofCode(404)
										.withBody(("Key '" + key + "' not found").getBytes(UTF_8));
							} catch (MalformedDataException e) {
								throw HttpError.ofCode(400, e);
							}
						}));
		if (!backupServiceOpt.isPresent()) {
			return servlet;
		}
		BackupService<K, S> backupService = backupServiceOpt.get();
		return servlet
				.map(POST, "/backup", request -> {
					if (backupService.backupInProgress()) {
						return Promise.of(HttpResponse.ofCode(403)
								.withBody("Backup is already in progress".getBytes(UTF_8)));
					}
					backupService.backup();
					return Promise.of(HttpResponse.ofCode(202));
				})
				.map(POST, "/awaitBackup", request ->
						backupService.backupInProgress() ?
								backupService.backup().map($ -> HttpResponse.ofCode(204)
										.withBody("Finished already running backup".getBytes(UTF_8))) :
								backupService.backup().map($ -> HttpResponse.ok200()));
	}
}
