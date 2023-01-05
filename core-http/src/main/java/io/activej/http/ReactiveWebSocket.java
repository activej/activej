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

import io.activej.async.function.AsyncRunnable;
import io.activej.async.function.AsyncSupplier;
import io.activej.async.process.AbstractAsyncCloseable;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.Checks;
import io.activej.common.recycle.Recyclable;
import io.activej.common.ref.Ref;
import io.activej.csp.AbstractChannelConsumer;
import io.activej.csp.AbstractChannelSupplier;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.http.AsyncWebSocket.Message.MessageType;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.CharacterCodingException;
import java.util.function.Consumer;

import static io.activej.bytebuf.ByteBuf.wrapForReading;
import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.nullify;
import static io.activej.csp.ChannelSuppliers.prefetch;
import static io.activej.http.HttpUtils.frameToMessageType;
import static io.activej.http.HttpUtils.getUTF8;
import static io.activej.http.AsyncWebSocket.Message.MessageType.TEXT;
import static io.activej.http.WebSocketConstants.*;
import static java.nio.charset.StandardCharsets.UTF_8;

final class ReactiveWebSocket extends AbstractAsyncCloseable implements AsyncWebSocket {
	private static final boolean CHECK = Checks.isEnabled(ReactiveWebSocket.class);

	private final HttpRequest request;
	private final HttpResponse response;
	private final Consumer<WebSocketException> onProtocolError;
	private final ChannelSupplier<Frame> frameInput;
	private final ChannelConsumer<Frame> frameOutput;
	private final int maxMessageSize;

	private @Nullable SettablePromise<?> readPromise;
	private @Nullable SettablePromise<Void> writePromise;

	ReactiveWebSocket(
			HttpRequest request,
			HttpResponse response,
			ChannelSupplier<Frame> frameInput,
			ChannelConsumer<Frame> frameOutput,
			Consumer<WebSocketException> onProtocolError,
			int maxMessageSize) {
		this.request = request;
		this.response = response;
		this.frameInput = prefetch(sanitize(frameInput));
		this.frameOutput = sanitize(frameOutput);
		this.onProtocolError = onProtocolError;
		this.maxMessageSize = maxMessageSize;
	}

	@Override
	public Promise<Message> readMessage() {
		return doRead(() -> {
			ByteBufs messageBufs = new ByteBufs();
			Ref<MessageType> typeRef = new Ref<>();
			return Promises.repeat(() -> frameInput.get()
					.then(frame -> {
						if (frame == null) {
							if (typeRef.get() == null) {
								return Promise.of(false);
							}
							// hence, all other exceptions would fail get() promise
							return Promise.ofException(REGULAR_CLOSE);
						}
						if (typeRef.get() == null) {
							typeRef.set(frameToMessageType(frame.getType()));
						}
						ByteBuf payload = frame.getPayload();
						if (messageBufs.remainingBytes() + payload.readRemaining() > maxMessageSize) {
							return protocolError(MESSAGE_TOO_BIG);
						}
						messageBufs.add(payload);
						return Promise.of(!frame.isLastFrame());
					}))
					.whenException(e -> messageBufs.recycle())
					.then($ -> {
						ByteBuf payload = messageBufs.takeRemaining();
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
		});
	}

	@Override
	public Promise<Frame> readFrame() {
		return doRead(frameInput::get);
	}

	@Override
	public Promise<Void> writeMessage(@Nullable Message msg) {
		return doWrite(() -> {
			if (msg == null) {
				return frameOutput.accept(null);
			}
			if (msg.getType() == TEXT) {
				return frameOutput.accept(Frame.text(wrapForReading(msg.getText().getBytes(UTF_8))));
			} else {
				return frameOutput.accept(Frame.binary(msg.getBuf()));
			}
		}, msg);
	}

	@Override
	public Promise<Void> writeFrame(@Nullable Frame frame) {
		return doWrite(() -> frameOutput.accept(frame), frame);
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
	protected void onClosed(Exception e) {
		frameOutput.closeEx(e);
		frameInput.closeEx(e);
		readPromise = nullify(readPromise, SettablePromise::setException, e);
		writePromise = nullify(writePromise, SettablePromise::setException, e);
	}

	@Override
	protected void onCleanup() {
		request.recycle();
		response.recycle();
	}

	private <T> Promise<T> protocolError(WebSocketException exception) {
		onProtocolError.accept(exception);
		closeEx(exception);
		return Promise.ofException(exception);
	}

	// region sanitizers
	private <T> Promise<T> doRead(AsyncSupplier<T> supplier) {
		if (CHECK) {
			checkState(inReactorThread());
			checkState(readPromise == null, "Concurrent reads");
		}

		if (isClosed()) return Promise.ofException(getException());

		SettablePromise<T> readPromise = new SettablePromise<>();
		this.readPromise = readPromise;
		supplier.get()
				.run((result, e) -> {
					this.readPromise = null;
					readPromise.trySet(result, e);
				});
		return readPromise;
	}

	private Promise<Void> doWrite(AsyncRunnable runnable, @Nullable Recyclable recyclable) {
		if (CHECK) {
			checkState(inReactorThread());
			checkState(writePromise == null, "Concurrent writes");
		}

		if (isClosed()) {
			if (recyclable != null) recyclable.recycle();
			return Promise.ofException(getException());
		}

		SettablePromise<Void> writePromise = new SettablePromise<>();
		this.writePromise = writePromise;
		runnable.run()
				.run((result, e) -> {
					this.writePromise = null;
					writePromise.trySet(result, e);
				});
		return writePromise;
	}

	private ChannelSupplier<Frame> sanitize(ChannelSupplier<Frame> supplier) {
		return new AbstractChannelSupplier<>(supplier) {
			@Override
			protected Promise<Frame> doGet() {
				return sanitize(supplier.get());
			}

			@Override
			protected void onClosed(Exception e) {
				ReactiveWebSocket.this.closeEx(e);
			}
		};
	}

	private ChannelConsumer<Frame> sanitize(ChannelConsumer<Frame> consumer) {
		return new AbstractChannelConsumer<>(consumer) {
			@Override
			protected Promise<Void> doAccept(@Nullable Frame value) {
				return sanitize(consumer.accept(value));
			}

			@Override
			protected void onClosed(Exception e) {
				ReactiveWebSocket.this.closeEx(e);
			}
		};
	}
	// endregion
}
