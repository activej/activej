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
import io.activej.crdt.storage.local.MapCrdtStorage;
import io.activej.http.*;
import io.activej.http.loader.IStaticLoader;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.json.JsonCodecFactory;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.types.TypeT;

import java.util.concurrent.Executor;

import static io.activej.http.HttpMethod.*;
import static io.activej.json.JsonUtils.fromJsonBytes;
import static io.activej.json.JsonUtils.toJsonBytes;
import static io.activej.launchers.initializers.Initializers.ofHttpServer;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class CrdtHttpModule<K extends Comparable<K>, S> extends AbstractModule {
	private final TypeT<CrdtData<K, S>> crdtDataManifest = new TypeT<>() {};

	@Provides
	HttpServer server(NioReactor reactor, AsyncServlet servlet, Config config) {
		return HttpServer.builder(reactor, servlet)
			.initialize(ofHttpServer(config.getChild("crdt.http")))
			.build();
	}

	@Provides
	IStaticLoader loader(Reactor reactor, Executor executor) {
		return IStaticLoader.ofClassPath(reactor, executor, "/");
	}

	@Provides
	AsyncServlet servlet(
		Reactor reactor,
		CrdtDescriptor<K, S> descriptor,
		MapCrdtStorage<K, S> client,
		OptionalDependency<BackupService<K, S>> backupServiceOpt,
		JsonCodecFactory jsonCodecFactory
	) {
		return RoutingServlet.builder(reactor)
			.with(POST, "/", request -> request.loadBody()
				.then(body -> {
					try {
						K key = fromJsonBytes(descriptor.keyJsonCodec(), body.getArray());
						S state = client.get(key);
						if (state != null) {
							return HttpResponse.ok200()
								.withBody(toJsonBytes(descriptor.stateJsonCodec(), state))
								.toPromise();
						}
						return HttpResponse.ofCode(404)
							.withBody(("Key '" + key + "' not found").getBytes(UTF_8))
							.toPromise();
					} catch (MalformedDataException e) {
						throw HttpError.ofCode(400, e);
					}
				}))
			.with(PUT, "/", request -> request.loadBody()
				.then(body -> {
					try {
						client.put(fromJsonBytes(jsonCodecFactory.resolve(crdtDataManifest), body.getArray()));
						return HttpResponse.ok200().toPromise();
					} catch (MalformedDataException e) {
						throw HttpError.ofCode(400, e);
					}
				}))
			.with(DELETE, "/", request -> request.loadBody()
				.then(body -> {
					try {
						K key = fromJsonBytes(descriptor.keyJsonCodec(), body.getArray());
						if (client.remove(key)) {
							return HttpResponse.ok200().toPromise();
						}
						return HttpResponse.ofCode(404)
							.withBody(("Key '" + key + "' not found").getBytes(UTF_8))
							.toPromise();
					} catch (MalformedDataException e) {
						throw HttpError.ofCode(400, e);
					}
				}))
			.initialize(builder -> {
				if (backupServiceOpt.isPresent()) {
					BackupService<K, S> backupService = backupServiceOpt.get();
					builder
						.with(POST, "/backup", request -> {
							if (backupService.backupInProgress()) {
								return HttpResponse.ofCode(403)
									.withBody("Backup is already in progress".getBytes(UTF_8))
									.toPromise();
							}
							backupService.backup();
							return HttpResponse.ofCode(202).toPromise();
						})
						.with(POST, "/awaitBackup", request ->
							backupService.backupInProgress() ?
								backupService.backup()
									.then($ -> HttpResponse.ofCode(204)
										.withBody("Finished already running backup".getBytes(UTF_8))
										.toPromise()) :
								backupService.backup()
									.then($ -> HttpResponse.ok200().toPromise()));
				}
			})
			.build();
	}
}
