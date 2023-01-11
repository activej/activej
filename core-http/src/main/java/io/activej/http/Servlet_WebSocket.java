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
import io.activej.csp.ChannelConsumers;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.queue.ChannelZeroBuffer;
import io.activej.promise.Promisable;
import io.activej.promise.Promise;

import java.util.Arrays;

import static io.activej.common.Checks.checkState;
import static io.activej.http.AbstractHttpConnection.WEB_SOCKET_VERSION;
import static io.activej.http.HttpHeaders.*;
import static io.activej.http.HttpUtils.getWebSocketAnswer;
import static io.activej.http.WebSocketConstants.NOT_A_WEB_SOCKET_REQUEST;
import static io.activej.http.WebSocketConstants.REGULAR_CLOSE;

/**
 * A servlet for handling web socket upgrade requests.
 * An implementation may inspect incoming HTTP request, based on which an implementation MUST call a provided function
 * with a corresponding HTTP response.
 * <p>
 * If a response has a code {@code 101} it is considered successful and the resulted promise of a web socket will be
 * completed with a {@link AsyncWebSocket}. A successful response must have no body or body stream.
 * <p>
 * If a response has code different than {@code 101}, it will be sent as is and the resulted promise will be completed
 * exceptionally.
 */
public abstract class Servlet_WebSocket implements AsyncServlet {
	protected Servlet_WebSocket() {
		checkState(AsyncWebSocket.ENABLED, "Web sockets are disabled by application settings");
	}

	protected Promisable<HttpResponse> onRequest(HttpRequest request) {
		return HttpResponse.ofCode(101);
	}

	protected abstract void onWebSocket(AsyncWebSocket webSocket);

	@Override
	public final Promise<HttpResponse> serve(HttpRequest request) {
		return validateHeaders(request)
				.then(() -> processAnswer(request))
				.then(answer -> {
					ChannelSupplier<ByteBuf> rawStream = request.takeBodyStream();
					assert rawStream != null;

					return onRequest(request)
							.promise()
							.whenException(e -> recycleStream(rawStream))
							.map(response -> {
								if (response.getCode() != 101) {
									recycleStream(rawStream);
									return response;
								}

								checkState(response.body == null && response.bodyStream == null, "Illegal body or stream");

								ChannelZeroBuffer<ByteBuf> buffer = new ChannelZeroBuffer<>();
								response.withBodyStream(buffer.getSupplier())
										.withHeader(UPGRADE, "Websocket")
										.withHeader(CONNECTION, "Upgrade")
										.withHeader(SEC_WEBSOCKET_ACCEPT, answer);

								WebSocketFramesToBufs encoder = WebSocketFramesToBufs.create(false);
								WebSocketBufsToFrames decoder = WebSocketBufsToFrames.create(
										request.maxBodySize,
										encoder::sendPong,
										ByteBuf::recycle,
										true);

								bindWebSocketTransformers(rawStream, encoder, decoder);

								onWebSocket(new WebSocket_Reactive(
										request,
										response,
										rawStream.transformWith(decoder),
										buffer.getConsumer().transformWith(encoder),
										decoder::onProtocolError,
										request.maxBodySize
								));

								return response;
							});
				});
	}

	private static void bindWebSocketTransformers(ChannelSupplier<ByteBuf> rawStream, WebSocketFramesToBufs encoder, WebSocketBufsToFrames decoder) {
		encoder.getCloseSentPromise()
				.then(decoder::getCloseReceivedPromise)
				.whenResult(rawStream::closeEx)
				.whenException(rawStream::closeEx);

		decoder.getProcessCompletion()
				.whenResult(() -> encoder.sendCloseFrame(REGULAR_CLOSE))
				.whenException(encoder::closeEx);
	}

	private static boolean isUpgradeHeaderMissing(HttpMessage message) {
		String headerValue = message.getHeader(HttpHeaders.CONNECTION);
		if (headerValue != null) {
			for (String val : headerValue.split(",")) {
				if ("upgrade".equalsIgnoreCase(val.trim())) {
					return false;
				}
			}
		}
		return true;
	}

	private static Promise<Void> validateHeaders(HttpRequest request) {
		if (isUpgradeHeaderMissing(request) ||
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

	private static void recycleStream(ChannelSupplier<ByteBuf> rawStream) {
		rawStream.streamTo(ChannelConsumers.recycling());
	}
}
