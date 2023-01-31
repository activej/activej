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

package io.activej.fs.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.ChannelConsumerTransformer;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.fs.FileMetadata;
import io.activej.fs.IFileSystem;
import io.activej.fs.exception.FileSystemException;
import io.activej.http.*;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.isBijection;
import static io.activej.csp.dsl.ChannelConsumerTransformer.identity;
import static io.activej.fs.http.FileSystemCommand.*;
import static io.activej.fs.util.JsonUtils.fromJson;
import static io.activej.fs.util.JsonUtils.toJson;
import static io.activej.fs.util.MessageTypes.STRING_META_MAP_TYPE;
import static io.activej.fs.util.RemoteFileSystemUtils.ofFixedSize;
import static io.activej.http.HttpHeaders.CONTENT_LENGTH;
import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * A client to the remote server with {@link FileSystemServlet}.
 * This client can be used to connect to publicly available servers.
 * <p>
 * Inherits all the limitations of {@link IFileSystem} implementation located on server.
 */
public final class HttpClientFileSystem extends AbstractReactive
		implements IFileSystem {
	private final IHttpClient client;
	private final String url;

	private HttpClientFileSystem(Reactor reactor, String url, IHttpClient client) {
		super(reactor);
		this.url = url;
		this.client = client;
	}

	public static HttpClientFileSystem create(Reactor reactor, String url, IHttpClient client) {
		return new HttpClientFileSystem(reactor, url.endsWith("/") ? url : url + '/', client);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name) {
		checkInReactorThread(this);
		return doUpload(name, null);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long size) {
		checkInReactorThread(this);
		return doUpload(name, size);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> append(String name, long offset) {
		checkInReactorThread(this);
		checkArgument(offset >= 0, "Offset cannot be less than 0");
		UrlBuilder urlBuilder = UrlBuilder.relative()
				.appendPathPart(APPEND)
				.appendPath(name);
		if (offset != 0) {
			urlBuilder.appendQuery("offset", offset);
		}
		return uploadData(HttpRequest.post(url + urlBuilder.build()), identity());
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long limit) {
		checkInReactorThread(this);
		checkArgument(offset >= 0 && limit >= 0);
		UrlBuilder urlBuilder = UrlBuilder.relative()
				.appendPathPart(DOWNLOAD)
				.appendPath(name);
		if (offset != 0) {
			urlBuilder.appendQuery("offset", offset);
		}
		if (limit != Long.MAX_VALUE) {
			urlBuilder.appendQuery("limit", limit);
		}
		return client.request(
						HttpRequest.get(
								url + urlBuilder
										.build()))
				.then(HttpClientFileSystem::checkResponse)
				.map(HttpMessage::takeBodyStream);
	}

	@Override
	public Promise<Map<String, FileMetadata>> list(String glob) {
		checkInReactorThread(this);
		return client.request(
						HttpRequest.get(
								url + UrlBuilder.relative()
										.appendPathPart(LIST)
										.appendQuery("glob", glob)
										.build()))
				.then(HttpClientFileSystem::checkResponse)
				.then(response -> response.loadBody())
				.map(body -> fromJson(STRING_META_MAP_TYPE, body));
	}

	@Override
	public Promise<@Nullable FileMetadata> info(String name) {
		checkInReactorThread(this);
		return client.request(
						HttpRequest.get(
								url + UrlBuilder.relative()
										.appendPathPart(INFO)
										.appendPathPart(name)
										.build()))
				.then(HttpClientFileSystem::checkResponse)
				.then(response -> response.loadBody())
				.map(body -> fromJson(FileMetadata.class, body));
	}

	@Override
	public Promise<Map<String, FileMetadata>> infoAll(Set<String> names) {
		checkInReactorThread(this);
		return client.request(
						HttpRequest.get(
										url + UrlBuilder.relative()
												.appendPathPart(INFO_ALL)
												.build())
								.withBody(toJson(names)))
				.then(HttpClientFileSystem::checkResponse)
				.then(response -> response.loadBody())
				.map(body -> fromJson(STRING_META_MAP_TYPE, body));
	}

	@Override
	public Promise<Void> ping() {
		checkInReactorThread(this);
		return client.request(
						HttpRequest.get(
								url + UrlBuilder.relative()
										.appendPathPart(PING)
										.build()))
				.then(HttpClientFileSystem::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> move(String name, String target) {
		checkInReactorThread(this);
		return client.request(
						HttpRequest.post(
								url + UrlBuilder.relative()
										.appendPathPart(MOVE)
										.appendQuery("name", name)
										.appendQuery("target", target)
										.build()))
				.then(HttpClientFileSystem::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		checkInReactorThread(this);
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return client.request(
						HttpRequest.post(
										url + UrlBuilder.relative()
												.appendPathPart(MOVE_ALL)
												.build())
								.withBody(toJson(sourceToTarget)))
				.then(HttpClientFileSystem::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> copy(String name, String target) {
		checkInReactorThread(this);
		return client.request(
						HttpRequest.post(
								url + UrlBuilder.relative()
										.appendPathPart(COPY)
										.appendQuery("name", name)
										.appendQuery("target", target)
										.build()))
				.then(HttpClientFileSystem::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		checkInReactorThread(this);
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return client.request(
						HttpRequest.post(
										url + UrlBuilder.relative()
												.appendPathPart(COPY_ALL)
												.build())
								.withBody(toJson(sourceToTarget)))
				.then(HttpClientFileSystem::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> delete(String name) {
		checkInReactorThread(this);
		return client.request(
						HttpRequest.of(HttpMethod.DELETE,
								url + UrlBuilder.relative()
										.appendPathPart(DELETE)
										.appendPath(name)
										.build()))
				.then(HttpClientFileSystem::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		checkInReactorThread(this);
		return client.request(
						HttpRequest.post(
										url + UrlBuilder.relative()
												.appendPathPart(DELETE_ALL)
												.build())
								.withBody(toJson(toDelete)))
				.then(HttpClientFileSystem::checkResponse)
				.toVoid();
	}

	private static Promise<HttpResponse> checkResponse(HttpResponse response) throws HttpError {
		return switch (response.getCode()) {
			case 200, 206 -> Promise.of(response);
			case 500 -> response.loadBody()
					.map(body -> {
						try {
							throw fromJson(FileSystemException.class, body);
						} catch (MalformedDataException ignored) {
							throw HttpError.ofCode(500);
						}
					});
			default -> throw HttpError.ofCode(response.getCode());
		};
	}

	private Promise<ChannelConsumer<ByteBuf>> doUpload(String filename, @Nullable Long size) {
		UrlBuilder urlBuilder = UrlBuilder.relative().appendPathPart(UPLOAD).appendPath(filename);
		HttpRequest request = HttpRequest.post(url + urlBuilder.build());

		if (size != null) {
			request.addHeader(CONTENT_LENGTH, String.valueOf(size));
		}

		return uploadData(request, size == null ? identity() : ofFixedSize(size));
	}

	private Promise<ChannelConsumer<ByteBuf>> uploadData(HttpRequest request, ChannelConsumerTransformer<ByteBuf, ChannelConsumer<ByteBuf>> transformer) {
		SettablePromise<ChannelConsumer<ByteBuf>> channelPromise = new SettablePromise<>();
		SettablePromise<HttpResponse> responsePromise = new SettablePromise<>();
		client.request(request
						.withBodyStream(ChannelSupplier.ofPromise(responsePromise
								.map(response -> {
									ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();
									ChannelConsumer<ByteBuf> consumer = buffer.getConsumer();
									channelPromise.trySet(consumer
											.transformWith(transformer)
											.withAcknowledgement(ack -> ack.both(response.loadBody()
													.map(body -> fromJson(UploadAcknowledgement.class, body))
													.whenResult(uploadAck -> !uploadAck.isOk(), ack1 -> {
														//noinspection ConstantConditions
														throw ack1.getError();
													})
													.whenException(e -> {
														channelPromise.trySetException(e);
														buffer.closeEx(e);
													}))));
									return buffer.getSupplier();
								}))))
				.then(HttpClientFileSystem::checkResponse)
				.whenException(channelPromise::trySetException)
				.whenComplete(responsePromise::trySet);

		return channelPromise;
	}

}
