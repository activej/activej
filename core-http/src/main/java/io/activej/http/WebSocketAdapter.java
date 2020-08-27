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

package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.annotation.Beta;
import io.activej.common.exception.UncheckedException;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.http.RoutingServlet.AsyncWebSocketServlet;
import io.activej.promise.Promisable;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.activej.common.Checks.checkState;
import static io.activej.http.AbstractHttpConnection.WEB_SOCKET_VERSION;
import static io.activej.http.HttpHeaders.*;
import static io.activej.http.HttpUtils.getWebSocketAnswer;
import static io.activej.http.HttpUtils.isHeaderMissing;
import static io.activej.http.WebSocketConstants.NOT_A_WEB_SOCKET_REQUEST;
import static io.activej.http.WebSocketConstants.REGULAR_CLOSE;

@Beta
final class WebSocketAdapter implements AsyncServlet {
	private final AsyncWebSocketServlet webSocketServlet;

	private WebSocketAdapter(AsyncWebSocketServlet webSocketServlet) {
		this.webSocketServlet = webSocketServlet;
	}

	static AsyncServlet webSocket(AsyncWebSocketServlet servlet) {
		return new WebSocketAdapter(servlet);
	}

	private static Promise<Void> validateHeaders(HttpRequest request) {
		if (isHeaderMissing(request, UPGRADE, "websocket") ||
				isHeaderMissing(request, CONNECTION, "upgrade") ||
				!Arrays.equals(WEB_SOCKET_VERSION, request.getHeader(SEC_WEBSOCKET_VERSION, ByteBuf::getArray))) {
			return Promise.ofException(NOT_A_WEB_SOCKET_REQUEST);
		}
		return Promise.complete();
	}

	private static Promise<String> processAnswer(HttpRequest request) {
		String header = request.getHeader(SEC_WEBSOCKET_KEY);
		if (header == null) {
			return Promise.ofException(NOT_A_WEB_SOCKET_REQUEST);
		}
		return Promise.of(getWebSocketAnswer(header.trim()));
	}

	@NotNull
	@Override
	public Promisable<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		return validateHeaders(request)
				.then(() -> processAnswer(request))
				.then(answer -> {
					SettablePromise<HttpResponse> successfulUpgrade = new SettablePromise<>();

					ChannelSupplier<ByteBuf> rawStream = request.getBodyStream();
					assert rawStream != null;

					webSocketServlet.serve(request, response -> {
						if (response.getCode() != 101) {
							successfulUpgrade.set(response);
							HttpException exception = HttpException.ofCode(response.getCode(), "Server did not respond with code 101");
							return Promise.ofException(exception);
						}

						checkState(response.body == null && response.bodyStream == null, "Illegal body or stream");

						ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();
						response.withBodyStream(buffer.getSupplier());

						successfulUpgrade.set(response
								.withHeader(UPGRADE, "Websocket")
								.withHeader(CONNECTION, "Upgrade")
								.withHeader(SEC_WEBSOCKET_ACCEPT, answer));

						WebSocketFramesToBufs encoder = WebSocketFramesToBufs.create(false);
						WebSocketBufsToFrames decoder = WebSocketBufsToFrames.create(request.maxBodySize, encoder, true);

						encoder.getCloseSentPromise()
								.then(decoder::getCloseReceivedPromise)
								.whenException(rawStream::closeEx)
								.whenResult(rawStream::closeEx);

						decoder.getProcessCompletion()
								.whenComplete(($, e) -> {
									if (e == null) {
										encoder.sendCloseFrame(REGULAR_CLOSE);
									} else {
										encoder.closeEx(e);
									}
								});

						return Promise.of(new WebSocketImpl(
								request,
								response,
								rawStream.transformWith(decoder),
								buffer.getConsumer().transformWith(encoder),
								decoder::onProtocolError,
								request.maxBodySize
								));
					});
					return successfulUpgrade;
				});
	}
}
