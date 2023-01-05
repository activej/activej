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
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.TruncatedDataException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelOutput;
import io.activej.csp.binary.BinaryChannelInput;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.csp.dsl.WithBinaryChannelInput;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.http.AsyncWebSocket.Frame;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;

import java.nio.charset.CharacterCodingException;
import java.util.function.Consumer;

import static io.activej.common.Checks.checkState;
import static io.activej.csp.binary.ByteBufsDecoder.ofFixedSize;
import static io.activej.http.HttpUtils.*;
import static io.activej.http.WebSocketConstants.*;
import static io.activej.http.WebSocketConstants.OpCode.*;

final class WebSocketBufsToFrames extends AbstractCommunicatingProcess
		implements WithChannelTransformer<WebSocketBufsToFrames, ByteBuf, Frame>, WithBinaryChannelInput<WebSocketBufsToFrames> {

	private static final byte OP_CODE_MASK = 0b00001111;
	private static final byte RSV_MASK = 0b01110000;
	private static final byte LAST_7_BITS_MASK = 0b01111111;

	private static final ByteBufsDecoder<Byte> SINGLE_BYTE_DECODER = bufs -> bufs.hasRemainingBytes(1) ? bufs.getByte() : null;

	private final long maxMessageSize;
	private final Consumer<ByteBuf> onPing;
	private final Consumer<ByteBuf> onPong;
	private final byte[] mask = new byte[4];
	private final boolean masked;
	private final SettablePromise<WebSocketException> closeReceivedPromise = new SettablePromise<>();

	private ByteBufs bufs;
	private BinaryChannelSupplier input;
	private ChannelConsumer<Frame> output;

	private int maskIndex;
	private boolean isFin;
	private boolean waitingForFin;
	private WebSocketConstants.OpCode currentOpCode;

	private final ByteBufs frameBufs = new ByteBufs();
	private final ByteBufs controlMessageBufs = new ByteBufs();

	// region creators
	WebSocketBufsToFrames(long maxMessageSize, Consumer<ByteBuf> onPing, Consumer<ByteBuf> onPong, boolean masked) {
		this.maxMessageSize = maxMessageSize;
		this.onPing = onPing;
		this.onPong = onPong;
		this.masked = masked;
	}

	public static WebSocketBufsToFrames create(long maxMessageSize, Consumer<ByteBuf> onPing, Consumer<ByteBuf> onPong, boolean maskRequired) {
		return new WebSocketBufsToFrames(maxMessageSize, onPing, onPong, maskRequired);
	}

	@Override
	public BinaryChannelInput getInput() {
		return input -> {
			checkState(this.input == null, "Input already set");
			this.input = sanitize(input);
			this.bufs = input.getBufs();
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public ChannelOutput<Frame> getOutput() {
		return output -> {
			checkState(this.output == null, "Output already set");
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}
	// endregion

	@Override
	protected void beforeProcess() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Output was not set");
	}

	public Promise<WebSocketException> getCloseReceivedPromise() {
		return closeReceivedPromise;
	}

	@Override
	protected void doProcess() {
		processOpCode();
	}

	private void processOpCode() {
		input.decode(SINGLE_BYTE_DECODER)
				.whenResult(firstByte -> {
					if ((firstByte & RSV_MASK) != 0) {
						onProtocolError(RESERVED_BITS_SET);
						return;
					}

					byte opCodeByte = (byte) (firstByte & OP_CODE_MASK);
					currentOpCode = fromOpCodeByte(opCodeByte);
					if (currentOpCode == null) {
						onProtocolError(UNKNOWN_OP_CODE);
						return;
					}

					isFin = firstByte < 0;
					if (currentOpCode.isControlCode()) {
						if (!isFin) {
							onProtocolError(FRAGMENTED_CONTROL_MESSAGE);
						} else {
							processLength();
						}
						return;
					}

					if (waitingForFin) {
						if (currentOpCode != OP_CONTINUATION) {
							onProtocolError(WAITING_FOR_LAST_FRAME);
							return;
						}
					} else {
						if (currentOpCode == OP_CONTINUATION) {
							onProtocolError(UNEXPECTED_CONTINUATION);
							return;
						}

					}
					waitingForFin = !isFin;

					processLength();
				});
	}

	private void processLength() {
		assert currentOpCode != null;

		input.decode(SINGLE_BYTE_DECODER)
				.whenResult(maskAndLen -> {
					boolean msgMasked = maskAndLen < 0;
					if (this.masked && !msgMasked) {
						onProtocolError(MASK_REQUIRED);
					}
					if (!this.masked && msgMasked) {
						onProtocolError(MASK_SHOULD_NOT_BE_PRESENT);
					}
					maskIndex = msgMasked ? 0 : -1;
					byte length = (byte) (maskAndLen & LAST_7_BITS_MASK);
					if (currentOpCode.isControlCode() && length > 125) {
						onProtocolError(INVALID_PAYLOAD_LENGTH);
						return;
					}
					if (length == 126) {
						processLength2(2);
					} else if (length == 127) {
						processLength2(8);
					} else {
						processMask(length);
					}
				});
	}

	private void processLength2(int numberOfBytes) {
		assert numberOfBytes == 2 || numberOfBytes == 8;
		input.decode(ofFixedSize(numberOfBytes))
				.whenResult(lenBuf -> {
					long len;
					if (numberOfBytes == 2) {
						len = Short.toUnsignedLong(lenBuf.readShort());
					} else {
						len = lenBuf.readLong();
						if (len < 0) {
							onProtocolError(INVALID_PAYLOAD_LENGTH);
						}
					}
					lenBuf.recycle();
					processMask(len);
				});
	}

	private void processMask(long length) {
		if (frameBufs.remainingBytes() + length > maxMessageSize) {
			onProtocolError(MESSAGE_TOO_BIG);
			return;
		}

		if (maskIndex == -1) {
			processPayload(length);
		} else {
			input.decode(bufs -> !bufs.hasRemainingBytes(4) ? null : bufs)
					.whenResult(bufs -> {
						bufs.drainTo(mask, 0, 4);
						processPayload(length);
					});
		}
	}

	private void processPayload(long length) {
		assert length >= 0;
		ByteBuf buf = bufs.takeAtMost(length > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) length);
		unmask(buf);
		long newLength = length - buf.readRemaining();
		if (buf.canRead()) {
			(currentOpCode.isControlCode() ? controlMessageBufs : frameBufs).add(buf);
		}
		if (newLength != 0) {
			Promise.complete()
					.then(() -> bufs.isEmpty() ? input.needMoreData() : Promise.complete())
					.whenResult(() -> processPayload(newLength));
			return;
		}

		if (currentOpCode.isControlCode()) {
			processControlPayload();
		} else {
			output.accept(new Frame(opToFrameType(currentOpCode), frameBufs.takeRemaining(), isFin))
					.whenResult(this::processOpCode);
		}
	}

	private void processControlPayload() {
		ByteBuf controlPayload = controlMessageBufs.takeRemaining();
		if (currentOpCode == OP_CLOSE) {
			frameBufs.recycle();
			if (controlPayload.canRead()) {
				int payloadLength = controlPayload.readRemaining();
				if (payloadLength < 2 || payloadLength > 125) {
					onProtocolError(INVALID_PAYLOAD_LENGTH);
					return;
				}
				int statusCode = Short.toUnsignedInt(controlPayload.readShort());
				if (isReservedCloseCode(statusCode)) {
					onProtocolError(INVALID_CLOSE_CODE);
					return;
				}
				String payload;
				try {
					payload = getUTF8(controlPayload);
				} catch (CharacterCodingException e) {
					onProtocolError(NOT_A_VALID_UTF_8);
					return;
				} finally {
					controlPayload.recycle();
				}
				if (statusCode == 1000) {
					output.acceptEndOfStream()
							.whenComplete(() -> closeReceivedPromise.trySet(REGULAR_CLOSE))
							.whenResult(this::completeProcess);
				} else {
					WebSocketException exception = new WebSocketException(statusCode, payload);
					onCloseReceived(exception);
				}
			} else {
				controlPayload.recycle();
				onCloseReceived(STATUS_CODE_MISSING);
			}
		} else if (currentOpCode == OP_PING) {
			onPing.accept(controlPayload);
			processOpCode();
		} else {
			assert currentOpCode == OP_PONG;
			onPong.accept(controlPayload);
			processOpCode();
		}
	}

	private void unmask(ByteBuf buf) {
		if (maskIndex == -1 || !buf.canRead()) {
			return;
		}
		for (int head = buf.head(); head < buf.tail(); head++) {
			buf.set(head, (byte) (buf.at(head) ^ mask[maskIndex++ % 4]));
		}
	}

	private void onCloseReceived(WebSocketException e) {
		closeReceivedPromise.trySet(e);
		closeEx(e);
	}

	void onProtocolError(WebSocketException e) {
		closeReceivedPromise.trySetException(e);
		closeEx(e);
	}

	@Override
	protected void doClose(Exception e) {
		if (output != null) {
			output.closeEx(e instanceof TruncatedDataException ? CLOSE_FRAME_MISSING : e);
		}
		frameBufs.recycle();
		controlMessageBufs.recycle();
	}

}
