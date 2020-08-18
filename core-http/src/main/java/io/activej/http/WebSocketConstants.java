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

import io.activej.common.annotation.Beta;
import io.activej.common.exception.StacklessException;
import org.jetbrains.annotations.Nullable;

@Beta
public final class WebSocketConstants {
	// region exceptions
	public static final WebSocketException EMPTY_CLOSE = new WebSocketException(WebSocketConstants.class);
	public static final WebSocketException REGULAR_CLOSE = new WebSocketException(WebSocketConstants.class, 1000);
	public static final WebSocketException CLOSE_EXCEPTION = new WebSocketException(WebSocketConstants.class, 1001, "Closed");
	public static final WebSocketException UNKNOWN_OP_CODE = new WebSocketException(WebSocketConstants.class, 1002, "Unknown op code");
	public static final WebSocketException RESERVED_BITS_SET = new WebSocketException(WebSocketConstants.class, 1002, "Reserved bits set, unknown extension");
	public static final WebSocketException FRAGMENTED_CONTROL_MESSAGE = new WebSocketException(WebSocketConstants.class, 1002, "Control messages should not be fragmented");
	public static final WebSocketException INVALID_CLOSE_CODE = new WebSocketException(WebSocketConstants.class, 1002, "Invalid close code");
	public static final WebSocketException WAITING_FOR_LAST_FRAME = new WebSocketException(WebSocketConstants.class, 1002, "Last frame has not been received yet");
	public static final WebSocketException UNEXPECTED_CONTINUATION = new WebSocketException(WebSocketConstants.class, 1002, "Received unexpected continuation frame");
	public static final WebSocketException INVALID_PAYLOAD_LENGTH = new WebSocketException(WebSocketConstants.class, 1002, "Invalid payload length");
	public static final WebSocketException MASK_REQUIRED = new WebSocketException(WebSocketConstants.class, 1002, "Message should be masked");
	public static final WebSocketException MASK_SHOULD_NOT_BE_PRESENT = new WebSocketException(WebSocketConstants.class, 1002, "Message should not be masked");
	public static final WebSocketException STATUS_CODE_MISSING = new WebSocketException(WebSocketConstants.class, 1005, "Status code missing");
	public static final WebSocketException CLOSE_FRAME_MISSING = new WebSocketException(WebSocketConstants.class, 1006, "Peer did not send CLOSE frame");
	public static final WebSocketException NOT_A_VALID_UTF_8 = new WebSocketException(WebSocketConstants.class, 1007, "Received TEXT message is not a valid UTF-8 message");
	public static final WebSocketException MESSAGE_TOO_BIG = new WebSocketException(WebSocketConstants.class, 1009, "Received message is too big");

	public static final StacklessException HANDSHAKE_FAILED = new StacklessException(WebSocketConstants.class, "Failed to perform a proper opening handshake");

	public static final HttpException NOT_A_WEB_SOCKET_REQUEST = HttpException.ofCode(400, "Not a websocket request");
	// endregion

	static final String MAGIC_STRING = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

	enum OpCode {
		OP_CONTINUATION((byte) 0x0),
		OP_TEXT((byte) 0x1),
		OP_BINARY((byte) 0x2),
		OP_CLOSE((byte) 0x8),
		OP_PING((byte) 0x9),
		OP_PONG((byte) 0xA);

		private final byte code;

		public byte getCode() {
			return code;
		}

		OpCode(byte code) {
			this.code = code;
		}

		boolean isControlCode() {
			return (code >> 2) != 0;
		}

		boolean isMessageCode() {
			return !isControlCode();
		}

		@Nullable
		static WebSocketConstants.OpCode fromOpCodeByte(byte b) {
			for (OpCode value : OpCode.values()) {
				if (value.code == b) {
					return value;
				}
			}
			return null;
		}
	}

	public enum FrameType {
		TEXT, BINARY, CONTINUATION
	}

	public enum MessageType {
		TEXT, BINARY
	}
}
