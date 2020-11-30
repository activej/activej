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
import io.activej.codec.StructuredDecoder;
import io.activej.common.exception.parse.ParseException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.ChannelConsumerTransformer;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.fs.ActiveFs;
import io.activej.fs.FileMetadata;
import io.activej.http.*;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.activej.codec.json.JsonUtils.fromJson;
import static io.activej.codec.json.JsonUtils.toJsonBuf;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.collection.CollectionUtils.isBijection;
import static io.activej.csp.dsl.ChannelConsumerTransformer.identity;
import static io.activej.fs.http.FsCommand.*;
import static io.activej.fs.util.Codecs.*;
import static io.activej.fs.util.RemoteFsUtils.ofFixedSize;
import static io.activej.http.HttpHeaders.CONTENT_LENGTH;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A client to the remote server with {@link ActiveFsServlet}.
 * This client can be used to connect to publicly available servers.
 * <p>
 * Inherits all of the limitations of {@link ActiveFs} implementation located on server.
 */
public final class HttpActiveFs implements ActiveFs {
	private final IAsyncHttpClient client;
	private final String url;

	private HttpActiveFs(String url, IAsyncHttpClient client) {
		this.url = url;
		this.client = client;
	}

	public static HttpActiveFs create(String url, IAsyncHttpClient client) {
		return new HttpActiveFs(url.endsWith("/") ? url : url + '/', client);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		return doUpload(name, null);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long size) {
		return doUpload(name, size);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> append(@NotNull String name, long offset) {
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
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long limit) {
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
				.then(HttpActiveFs::checkResponse)
				.map(HttpMessage::getBodyStream);
	}

	@Override
	public Promise<Map<String, FileMetadata>> list(@NotNull String glob) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(LIST)
								.appendQuery("glob", glob)
								.build()))
				.then(HttpActiveFs::checkResponse)
				.then(HttpMessage::loadBody)
				.then(parseBody(FILE_META_MAP_CODEC));
	}

	@Override
	public Promise<@Nullable FileMetadata> info(@NotNull String name) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(INFO)
								.appendPathPart(name)
								.build()))
				.then(HttpActiveFs::checkResponse)
				.then(HttpMessage::loadBody)
				.then(parseBody(FILE_META_CODEC_NULLABLE));
	}

	@Override
	public Promise<Map<String, @NotNull FileMetadata>> infoAll(@NotNull Set<String> names) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(INFO_ALL)
								.build())
						.withBody(toJsonBuf(STRINGS_SET_CODEC, names)))
				.then(HttpActiveFs::checkResponse)
				.then(HttpMessage::loadBody)
				.then(parseBody(FILE_META_MAP_CODEC));
	}

	@Override
	public Promise<Void> ping() {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(PING)
								.build()))
				.then(HttpActiveFs::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target) {
		return client.request(
				HttpRequest.post(
						url + UrlBuilder.relative()
								.appendPathPart(MOVE)
								.appendQuery("name", name)
								.appendQuery("target", target)
								.build()))
				.then(HttpActiveFs::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return client.request(
				HttpRequest.post(
						url + UrlBuilder.relative()
								.appendPathPart(MOVE_ALL)
								.build())
						.withBody(toJsonBuf(SOURCE_TO_TARGET_CODEC, sourceToTarget)))
				.then(HttpActiveFs::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return client.request(
				HttpRequest.post(
						url + UrlBuilder.relative()
								.appendPathPart(COPY)
								.appendQuery("name", name)
								.appendQuery("target", target)
								.build()))
				.then(HttpActiveFs::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return client.request(
				HttpRequest.post(
						url + UrlBuilder.relative()
								.appendPathPart(COPY_ALL)
								.build())
						.withBody(toJsonBuf(SOURCE_TO_TARGET_CODEC, sourceToTarget)))
				.then(HttpActiveFs::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> delete(@NotNull String name) {
		return client.request(
				HttpRequest.of(HttpMethod.DELETE,
						url + UrlBuilder.relative()
								.appendPathPart(DELETE)
								.appendPath(name)
								.build()))
				.then(HttpActiveFs::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		return client.request(
				HttpRequest.post(
						url + UrlBuilder.relative()
								.appendPathPart(DELETE_ALL)
								.build())
						.withBody(toJsonBuf(STRINGS_SET_CODEC, toDelete)))
				.then(HttpActiveFs::checkResponse)
				.toVoid();
	}

	private static Promise<HttpResponse> checkResponse(HttpResponse response) {
		switch (response.getCode()) {
			case 200:
			case 206:
				return Promise.of(response);
			case 500:
				return response.loadBody()
						.then(body -> {
							try {
								return Promise.ofException(fromJson(FS_EXCEPTION_CODEC, body.getString(UTF_8)));
							} catch (ParseException ignored) {
								return Promise.ofException(HttpException.ofCode(500));
							}
						});
			default:
				return Promise.ofException(HttpException.ofCode(response.getCode()));
		}
	}

	private static <T> Function<ByteBuf, Promise<T>> parseBody(StructuredDecoder<T> decoder) {
		return body -> {
			try {
				return Promise.of(fromJson(decoder, body.getString(UTF_8)));
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		};
	}

	@NotNull
	private Promise<ChannelConsumer<ByteBuf>> doUpload(@NotNull String filename, @Nullable Long size) {
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
											.then(parseBody(UploadAcknowledgement.CODEC))
											.then(HttpActiveFs::failOnException)
											.whenException(e -> {
												channelPromise.trySetException(e);
												buffer.closeEx(e);
											}))));
							return buffer.getSupplier();
						}))))
				.then(HttpActiveFs::checkResponse)
				.whenException(channelPromise::trySetException)
				.whenComplete(responsePromise::trySet);

		return channelPromise;
	}

	private static Promise<Void> failOnException(UploadAcknowledgement ack) {
		if (ack.isOk()) {
			return Promise.complete();
		}
		//noinspection ConstantConditions - checked above
		return Promise.ofException(ack.getError());
	}

}
