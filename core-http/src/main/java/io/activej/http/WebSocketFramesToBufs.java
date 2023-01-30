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

import io.activej.async.exception.AsyncCloseException;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.common.Checks;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelOutput;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.http.IWebSocket.Frame;
import io.activej.http.IWebSocket.Frame.FrameType;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ThreadLocalRandom;

import static io.activej.common.Checks.checkState;
import static io.activej.http.HttpUtils.frameToOpType;
import static io.activej.http.IWebSocket.Frame.FrameType.*;
import static io.activej.http.WebSocketConstants.*;
import static io.activej.http.WebSocketConstants.OpCode.OP_CLOSE;
import static io.activej.http.WebSocketConstants.OpCode.OP_PONG;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class WebSocketFramesToBufs extends AbstractCommunicatingProcess
		implements WithChannelTransformer<WebSocketFramesToBufs, Frame, ByteBuf> {
	private static final boolean CHECKS = Checks.isEnabled(WebSocketFramesToBufs.class);

	private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

	private final boolean masked;
	private final SettablePromise<Void> closeSentPromise = new SettablePromise<>();

	private ChannelSupplier<Frame> input;
	private ChannelConsumer<ByteBuf> output;

	private @Nullable Promise<Void> pendingPromise;
	private boolean closing;
	private boolean waitingForFin;

	private WebSocketFramesToBufs(boolean masked) {
		this.masked = masked;
	}

	public static WebSocketFramesToBufs create(boolean masked) {
		return new WebSocketFramesToBufs(masked);
	}

	@SuppressWarnings("ConstantConditions") //check input for clarity
	@Override
	public ChannelInput<Frame> getInput() {
		return input -> {
			checkInReactorThread(this);
			checkState(this.input == null, "Input already set");
			this.input = sanitize(input);
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			checkInReactorThread(this);
			checkState(this.output == null, "Output already set");
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}

	@Override
	protected void beforeProcess() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Output was not set");
	}

	@Override
	protected void doProcess() {
		input.streamTo(ChannelConsumer.of(
						frame -> {
							if (CHECKS) checkFrameOrder(frame);

							return doAccept(encodeData(frame));
						}))
				.then(() -> sendCloseFrame(REGULAR_CLOSE))
				.whenResult(this::completeProcess);
	}

	private ByteBuf doEncode(ByteBuf payload, OpCode opCode, boolean isLastFrame) {
		int bufSize = payload.readRemaining();
		int lenSize = bufSize < 126 ? 1 : bufSize < 65536 ? 3 : 9;

		ByteBuf framedBuf = ByteBufPool.allocate(1 + lenSize + (masked ? 4 : 0) + bufSize);
		framedBuf.writeByte(isLastFrame ? (byte) (opCode.getCode() | 0x80) : opCode.getCode());
		if (lenSize == 1) {
			framedBuf.writeByte((byte) bufSize);
		} else if (lenSize == 3) {
			framedBuf.writeByte((byte) 126);
			framedBuf.writeShort((short) bufSize);
		} else {
			framedBuf.writeByte((byte) 127);
			framedBuf.writeLong(bufSize);
		}
		if (masked) {
			int idx = framedBuf.head() + 1;
			framedBuf.set(idx, (byte) (framedBuf.at(idx) | 0x80));
			byte[] mask = new byte[4];
			RANDOM.nextBytes(mask);
			framedBuf.put(mask);
			for (int i = 0, head = payload.head(); head < payload.tail(); head++) {
				payload.set(head, (byte) (payload.at(head) ^ mask[i++ % 4]));
			}
		}
		framedBuf.put(payload);
		payload.recycle();
		return framedBuf;
	}

	private ByteBuf encodeData(Frame frame) {
		return doEncode(frame.getPayload(), frameToOpType(frame.getType()), frame.isLastFrame());
	}

	private ByteBuf encodePong(ByteBuf buf) {
		return doEncode(buf, OP_PONG, true);
	}

	private ByteBuf encodeClose(WebSocketException e) {
		Integer code = e.getCode();
		String reason = e.getReason();
		ByteBuf closePayload = ByteBufPool.allocate(code == null ? 0 : (2 + reason.length()));
		if (code != null) {
			closePayload.writeShort(code.shortValue());
		}
		if (!reason.isEmpty()) {
			ByteBuf reasonBuf = ByteBufStrings.wrapUtf8(reason);
			closePayload.put(reasonBuf);
			reasonBuf.recycle();
		}
		return doEncode(closePayload, OP_CLOSE, true);
	}

	public Promise<Void> getCloseSentPromise() {
		return closeSentPromise;
	}

	private Promise<Void> doAccept(@Nullable ByteBuf buf) {
		if (closeSentPromise.isComplete()) {
			if (buf != null) buf.recycle();
			return Promise.ofException(new AsyncCloseException());
		}
		if (pendingPromise == null) {
			return pendingPromise = output.accept(buf);
		} else {
			Promise<Void> pendingPromise = this.pendingPromise;
			this.pendingPromise = null;
			return this.pendingPromise = pendingPromise.then(() -> output.accept(buf));
		}
	}

	void sendPong(ByteBuf payload) {
		checkInReactorThread(this);
		doAccept(encodePong(payload));
	}

	Promise<Void> sendCloseFrame(WebSocketException e) {
		checkInReactorThread(this);
		if (closing) return Promise.complete();
		closing = true;
		return doAccept(encodeClose(e == STATUS_CODE_MISSING ? EMPTY_CLOSE : e))
				.then(() -> doAccept(null))
				.whenComplete(() -> closeSentPromise.trySet(null));
	}

	private void checkFrameOrder(Frame frame) {
		FrameType type = frame.getType();
		if (!waitingForFin) {
			checkState(type == TEXT || type == BINARY);
			if (!frame.isLastFrame()) waitingForFin = true;
		} else {
			checkState(type == CONTINUATION);
			if (frame.isLastFrame()) waitingForFin = false;
		}
	}

	@Override
	protected void doClose(Exception e) {
		if (output == null || input == null) return;

		WebSocketException exception;
		if (e instanceof WebSocketException wsEx) {
			if (wsEx.canBeEchoed()) {
				exception = wsEx;
			} else {
				Integer code = wsEx.getCode();
				assert code != null;
				exception = code == 1005 ?
						EMPTY_CLOSE :
						masked ? GOING_AWAY : SERVER_ERROR;
			}
		} else {
			exception = masked ? GOING_AWAY : SERVER_ERROR;
		}
		sendCloseFrame(exception);
	}

}
