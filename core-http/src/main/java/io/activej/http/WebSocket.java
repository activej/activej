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
import io.activej.csp.consumer.AbstractChannelConsumer;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.http.IWebSocket.Message.MessageType;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettableCallback;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.CharacterCodingException;
import java.util.function.Consumer;

import static io.activej.bytebuf.ByteBuf.wrapForReading;
import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.nullify;
import static io.activej.csp.supplier.ChannelSuppliers.prefetch;
import static io.activej.http.HttpUtils.frameToMessageType;
import static io.activej.http.HttpUtils.getUTF8;
import static io.activej.http.IWebSocket.Message.MessageType.TEXT;
import static io.activej.http.WebSocketConstants.*;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class WebSocket extends AbstractAsyncCloseable implements IWebSocket {
	private static final boolean CHECKS = Checks.isEnabled(WebSocket.class);

	private final HttpRequest request;
	private final HttpResponse response;
	private final Consumer<WebSocketException> onProtocolError;
	private final ChannelSupplier<Frame> frameInput;
	private final ChannelConsumer<Frame> frameOutput;
	private final int maxMessageSize;

	private @Nullable SettablePromise<?> readPromise;
	private @Nullable SettablePromise<Void> writePromise;

	WebSocket(
		HttpRequest request,
		HttpResponse response,
		ChannelSupplier<Frame> frameInput,
		ChannelConsumer<Frame> frameOutput,
		Consumer<WebSocketException> onProtocolError,
		int maxMessageSize
	) {
		this.request = request;
		this.response = response;
		this.frameInput = prefetch(sanitize(frameInput));
		this.frameOutput = sanitize(frameOutput);
		this.onProtocolError = onProtocolError;
		this.maxMessageSize = maxMessageSize;
	}

	@Override
	public Promise<Message> readMessage() {
		if (CHECKS) checkInReactorThread(this);
		return doRead(() -> {
			ByteBufs messageBufs = new ByteBufs();
			Ref<MessageType> typeRef = new Ref<>();
			return Promises.repeat(() -> frameInput.get()
					.thenCallback((frame, cb) -> {
						if (frame == null) {
							if (typeRef.get() == null) {
								cb.set(false);
								return;
							}
							// hence, all other exceptions would fail get() promise
							cb.setException(REGULAR_CLOSE);
							return;
						}
						if (typeRef.get() == null) {
							typeRef.set(frameToMessageType(frame.getType()));
						}
						ByteBuf payload = frame.getPayload();
						if (messageBufs.remainingBytes() + payload.readRemaining() > maxMessageSize) {
							protocolError(MESSAGE_TOO_BIG, cb);
							return;
						}
						messageBufs.add(payload);
						cb.set(!frame.isLastFrame());
					}))
				.whenException(e -> messageBufs.recycle())
				.thenCallback(($, cb) -> {
					ByteBuf payload = messageBufs.takeRemaining();
					MessageType type = typeRef.get();
					if (type == MessageType.TEXT) {
						try {
							cb.set(Message.text(getUTF8(payload)));
						} catch (CharacterCodingException e) {
							protocolError(NOT_A_VALID_UTF_8, cb);
						} finally {
							payload.recycle();
						}
					} else if (type == MessageType.BINARY) {
						cb.set(Message.binary(payload));
					} else {
						cb.set(null);
					}
				});
		});
	}

	@Override
	public Promise<Frame> readFrame() {
		if (CHECKS) checkInReactorThread(this);
		return doRead(frameInput::get);
	}

	@Override
	public Promise<Void> writeMessage(@Nullable Message msg) {
		if (CHECKS) checkInReactorThread(this);
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
		if (CHECKS) checkInReactorThread(this);
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

	private void protocolError(WebSocketException exception, SettableCallback<?> cb) {
		onProtocolError.accept(exception);
		closeEx(exception);
		cb.setException(exception);
	}

	// region sanitizers
	private <T> Promise<T> doRead(AsyncSupplier<T> supplier) {
		checkState(readPromise == null, "Concurrent reads");

		if (isClosed()) return Promise.ofException(getException());

		SettablePromise<T> readPromise = new SettablePromise<>();
		this.readPromise = readPromise;
		supplier.get()
			.subscribe((result, e) -> {
				this.readPromise = null;
				readPromise.trySet(result, e);
			});
		return readPromise;
	}

	private Promise<Void> doWrite(AsyncRunnable runnable, @Nullable Recyclable recyclable) {
		assert reactor.inReactorThread();
		checkState(writePromise == null, "Concurrent writes");

		if (isClosed()) {
			if (recyclable != null) recyclable.recycle();
			return Promise.ofException(getException());
		}

		SettablePromise<Void> writePromise = new SettablePromise<>();
		this.writePromise = writePromise;
		runnable.run()
			.subscribe((result, e) -> {
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
				WebSocket.this.closeEx(e);
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
				WebSocket.this.closeEx(e);
			}
		};
	}
	// endregion
}
