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

package io.activej.uikernel;

import com.google.gson.Gson;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.common.exception.MalformedDataException;
import io.activej.http.*;
import io.activej.reactor.Reactor;

import java.util.List;

import static io.activej.http.HttpHeaderValue.ofContentType;
import static io.activej.http.HttpHeaders.CONTENT_TYPE;
import static io.activej.http.HttpMethod.*;
import static io.activej.uikernel.Utils.deserializeUpdateRequest;
import static io.activej.uikernel.Utils.fromJson;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Rest API for UiKernel Tables
 */
public class UiKernelServlets {
	private static final ContentType JSON_UTF8 = ContentType.of(MediaTypes.JSON, UTF_8);

	private static final String ID_PARAMETER_NAME = "id";

	public static <K, R extends AbstractRecord<K>> RoutingServlet apiServlet(Reactor reactor, IGridModel<K, R> model, Gson gson) {
		return RoutingServlet.create(reactor)
				.map(POST, "/", create(model, gson))
				.map(GET, "/", read(model, gson))
				.map(PUT, "/", update(model, gson))
				.map(DELETE, "/:" + ID_PARAMETER_NAME, delete(model, gson))
				.map(GET, "/:" + ID_PARAMETER_NAME, get(model, gson));
	}

	public static <K, R extends AbstractRecord<K>> AsyncServlet read(IGridModel<K, R> model, Gson gson) {
		return request -> {
			try {
				ReadSettings<K> settings = ReadSettings.from(gson, request);
				return model.read(settings)
						.map(response ->
								createResponse(response.toJson(gson, model.getRecordType(), model.getIdType())));
			} catch (MalformedDataException | MalformedHttpException e) {
				throw HttpError.ofCode(400, e);
			}
		};
	}

	public static <K, R extends AbstractRecord<K>> AsyncServlet get(IGridModel<K, R> model, Gson gson) {
		return request -> {
			try {
				ReadSettings<K> settings = ReadSettings.from(gson, request);
				K id = fromJson(gson, request.getPathParameter(ID_PARAMETER_NAME), model.getIdType());
				return model.read(id, settings)
						.map(obj ->
								createResponse(gson.toJson(obj, model.getRecordType())));
			} catch (MalformedDataException | MalformedHttpException e) {
				throw HttpError.ofCode(400, e);
			}
		};
	}

	public static <K, R extends AbstractRecord<K>> AsyncServlet create(IGridModel<K, R> model, Gson gson) {
		return request -> request.loadBody()
				.then(body -> {
					try {
						String json = body.getString(UTF_8);
						R obj = fromJson(gson, json, model.getRecordType());
						return model.create(obj)
								.map(response ->
										createResponse(response.toJson(gson, model.getIdType())));
					} catch (MalformedDataException e) {
						throw HttpError.ofCode(400, e);
					}
				});
	}

	public static <K, R extends AbstractRecord<K>> AsyncServlet update(IGridModel<K, R> model, Gson gson) {
		return request -> request.loadBody()
				.then(body -> {
					try {
						String json = body.getString(UTF_8);
						List<R> list = deserializeUpdateRequest(gson, json, model.getRecordType(), model.getIdType());
						return model.update(list)
								.map(result ->
										createResponse(result.toJson(gson, model.getRecordType(), model.getIdType())));
					} catch (MalformedDataException e) {
						throw HttpError.ofCode(400, e);
					}
				});
	}

	public static <K, R extends AbstractRecord<K>> AsyncServlet delete(IGridModel<K, R> model, Gson gson) {
		return request -> {
			try {
				K id = fromJson(gson, request.getPathParameter("id"), model.getIdType());
				return model.delete(id)
						.map(response -> {
							HttpResponse.Builder builder = HttpResponse.Builder.ok200();
							if (response.hasErrors()) {
								String json = gson.toJson(response.getErrors());
								builder.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF8));
								builder.withBody(ByteBufStrings.wrapUtf8(json));
							}
							return builder.build();
						});
			} catch (MalformedDataException e) {
				throw HttpError.ofCode(400, e);
			}
		};
	}

	private static HttpResponse createResponse(String body) {
		return HttpResponse.Builder.ok200()
				.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF8))
				.withBody(ByteBufStrings.wrapUtf8(body))
				.build();
	}
}
