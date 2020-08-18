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
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.annotation.Beta;
import io.activej.common.ref.Ref;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.http.WebSocketConstants.*;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.CharacterCodingException;
import java.util.function.Consumer;

import static io.activej.bytebuf.ByteBuf.wrapForReading;
import static io.activej.http.HttpUtils.frameToMessageType;
import static io.activej.http.HttpUtils.getUTF8;
import static io.activej.http.WebSocketConstants.*;

@Beta
final class WebSocketImpl implements WebSocket {
	private final HttpRequest request;
	private final HttpResponse response;
	private final Consumer<WebSocketException> onProtocolError;
	private final ChannelSupplier<Frame> frameInput;
	private final ChannelConsumer<Frame> frameOutput;
	private final int maxMessageSize;

	WebSocketImpl(
			HttpRequest request,
			HttpResponse response,
			ChannelSupplier<Frame> frameInput,
			ChannelConsumer<Frame> frameOutput,
			Consumer<WebSocketException> onProtocolError,
			int maxMessageSize) {
		this.request = request;
		this.response = response;
		this.frameInput = frameInput;
		this.frameOutput = frameOutput;
		this.onProtocolError = onProtocolError;
		this.maxMessageSize = maxMessageSize;
	}

	@Override
	@NotNull
	public Promise<Message> readMessage() {
		ByteBufQueue messageQueue = new ByteBufQueue();
		Ref<MessageType> typeRef = new Ref<>();
		return Promises.repeat(() -> frameInput.get()
				.then(frame -> {
					if (frame == null) {
						if (typeRef.get() == null) {
							return Promise.of(false);
						}
						// hence all other exceptions would fail get() promise
						return Promise.ofException(REGULAR_CLOSE);
					}
					if (typeRef.get() == null) {
						typeRef.set(frameToMessageType(frame.getType()));
					}
					ByteBuf payload = frame.getPayload();
					if (messageQueue.remainingBytes() + payload.readRemaining() > maxMessageSize){
						return protocolError(MESSAGE_TOO_BIG);
					}
					messageQueue.add(payload);
					return Promise.of(!frame.isLastFrame());
				}))
				.whenException(e -> messageQueue.recycle())
				.then($ -> {
					ByteBuf payload = messageQueue.takeRemaining();
					MessageType type = typeRef.get();
					if (type == MessageType.TEXT) {
						try {
							return Promise.of(Message.text(getUTF8(payload)));
						} catch (CharacterCodingException e) {
							return protocolError(NOT_A_VALID_UTF_8);
						} finally {
							payload.recycle();
						}
					} else if (type == MessageType.BINARY) {
						return Promise.of(Message.binary(payload));
					} else {
						return Promise.of(null);
					}
				});
	}

	@Override
	@NotNull
	public Promise<Frame> readFrame() {
		return frameInput.get();
	}

	@Override
	@NotNull
	public Promise<Void> writeMessage(@Nullable Message msg) {
		if (msg == null) {
			return frameOutput.accept(null);
		}
		if (msg.getType() == MessageType.TEXT) {
			return frameOutput.accept(Frame.text(wrapForReading(msg.getText().getBytes())));
		} else {
			return frameOutput.accept(Frame.binary(msg.getBuf()));
		}
	}

	@Override
	@NotNull
	public Promise<Void> writeFrame(@Nullable Frame frame) {
		return frameOutput.accept(frame);
	}

	@Override
	public HttpRequest getRequest() {
		return request;
	}

	@Override
	public HttpResponse getResponse() {
		return response;
	}

	@Override
	public void closeEx(@NotNull Throwable exception) {
		frameOutput.closeEx(exception);
		frameInput.closeEx(exception);
	}

	private <T> Promise<T> protocolError(WebSocketException exception){
		onProtocolError.accept(exception);
		return Promise.ofException(exception);
	}
}
