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

package io.activej.remotefs.http;


import io.activej.bytebuf.ByteBuf;
import io.activej.codec.StructuredDecoder;
import io.activej.common.exception.StacklessException;
import io.activej.common.exception.parse.ParseException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.http.*;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.remotefs.FileMetadata;
import io.activej.remotefs.FsClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.activej.codec.json.JsonUtils.fromJson;
import static io.activej.codec.json.JsonUtils.toJsonBuf;
import static io.activej.common.Preconditions.checkArgument;
import static io.activej.remotefs.http.FsCommand.*;
import static io.activej.remotefs.http.UploadAcknowledgement.Status.OK;
import static io.activej.remotefs.util.Codecs.*;
import static io.activej.remotefs.util.RemoteFsUtils.ID_TO_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HttpFsClient implements FsClient {
	public static final StacklessException UNKNOWN_SERVER_ERROR = new StacklessException(HttpFsClient.class, "Unknown server error occurred");

	private final IAsyncHttpClient client;
	private final String url;

	private HttpFsClient(String url, IAsyncHttpClient client) {
		this.url = url;
		this.client = client;
	}

	public static HttpFsClient create(String url, IAsyncHttpClient client) {
		return new HttpFsClient(url.endsWith("/") ? url : url + '/', client);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String filename) {
		SettablePromise<ChannelConsumer<ByteBuf>> channelPromise = new SettablePromise<>();
		SettablePromise<HttpResponse> responsePromise = new SettablePromise<>();

		UrlBuilder urlBuilder = UrlBuilder.relative().appendPathPart(UPLOAD).appendPath(filename);

		client.request(
				HttpRequest.post(url + urlBuilder.build())
						.withBodyStream(ChannelSupplier.ofPromise(responsePromise
								.map(response -> {
									ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();
									channelPromise.trySet(buffer.getConsumer()
											.withAcknowledgement(ack -> ack.both(response.loadBody()
													.then(parseBody(UploadAcknowledgement.CODEC))
													.then(HttpFsClient::failOnException)
													.whenException(e -> {
														channelPromise.trySetException(e);
														buffer.closeEx(e);
													}))));
									return buffer.getSupplier();
								}))))
				.then(HttpFsClient::checkResponse)
				.whenException(channelPromise::trySetException)
				.whenComplete(responsePromise::trySet);

		return channelPromise;
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
				.then(HttpFsClient::checkResponse)
				.map(HttpMessage::getBodyStream);
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(LIST)
								.appendQuery("glob", glob)
								.build()))
				.then(HttpFsClient::checkResponse)
				.then(HttpMessage::loadBody)
				.then(parseBody(FILE_META_LIST_CODEC));
	}

	@Override
	public Promise<@Nullable FileMetadata> info(@NotNull String name) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(INFO)
								.appendPathPart(name)
								.build()))
				.then(HttpFsClient::checkResponse)
				.then(HttpMessage::loadBody)
				.then(parseBody(FILE_META_CODEC_NULLABLE));
	}

	@Override
	public Promise<Map<String, @Nullable FileMetadata>> infoAll(@NotNull List<String> names) {
		return client.request(
				HttpRequest.get(
						url + UrlBuilder.relative()
								.appendPathPart(INFO_ALL)
								.build())
						.withBody(toJsonBuf(NAMES_CODEC, names)))
				.then(HttpFsClient::checkResponse)
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
				.then(HttpFsClient::checkResponse)
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
				.then(HttpFsClient::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		return client.request(
				HttpRequest.post(
						url + UrlBuilder.relative()
								.appendPathPart(MOVE_ALL)
								.build())
						.withBody(toJsonBuf(SOURCE_TO_TARGET_CODEC, sourceToTarget)))
				.then(HttpFsClient::checkResponse)
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
				.then(HttpFsClient::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		return client.request(
				HttpRequest.post(
						url + UrlBuilder.relative()
								.appendPathPart(COPY_ALL)
								.build())
						.withBody(toJsonBuf(SOURCE_TO_TARGET_CODEC, sourceToTarget)))
				.then(HttpFsClient::checkResponse)
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
				.then(HttpFsClient::checkResponse)
				.toVoid();
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		return client.request(
				HttpRequest.post(
						url + UrlBuilder.relative()
								.appendPathPart(DELETE_ALL)
								.build())
						.withBody(toJsonBuf(TO_DELETE_CODEC, toDelete)))
				.then(HttpFsClient::checkResponse)
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
								Integer code = fromJson(ERROR_CODE_CODEC, body.getString(UTF_8)).getValue1();
								return Promise.ofException(ID_TO_ERROR.getOrDefault(code, HttpException.ofCode(500)));
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

	public static Promise<Void> failOnException(UploadAcknowledgement ack) {
		if (ack.getStatus() == OK) {
			return Promise.complete();
		}
		Integer errorCode = ack.getErrorCode();
		if (errorCode != null) {
			Throwable exception = ID_TO_ERROR.get(errorCode);
			if (exception != null) {
				return Promise.ofException(exception);
			}
		}
		return Promise.ofException(UNKNOWN_SERVER_ERROR);
	}

}
