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

import org.jetbrains.annotations.Nullable;

public final class WebSocketConstants {
	// region exceptions
	static final WebSocketException EMPTY_CLOSE = new WebSocketException();
	static final WebSocketException REGULAR_CLOSE = new WebSocketException(1000);
	static final WebSocketException GOING_AWAY = new WebSocketException(1001, "Client going away");
	static final WebSocketException UNKNOWN_OP_CODE = new WebSocketException(1002, "Unknown op code");
	static final WebSocketException RESERVED_BITS_SET = new WebSocketException(1002, "Reserved bits set, unknown extension");
	static final WebSocketException FRAGMENTED_CONTROL_MESSAGE = new WebSocketException(1002, "Control messages should not be fragmented");
	static final WebSocketException INVALID_CLOSE_CODE = new WebSocketException(1002, "Invalid close code");
	static final WebSocketException WAITING_FOR_LAST_FRAME = new WebSocketException(1002, "Last frame has not been received yet");
	static final WebSocketException UNEXPECTED_CONTINUATION = new WebSocketException(1002, "Received unexpected continuation frame");
	static final WebSocketException INVALID_PAYLOAD_LENGTH = new WebSocketException(1002, "Invalid payload length");
	static final WebSocketException MASK_REQUIRED = new WebSocketException(1002, "Message should be masked");
	static final WebSocketException MASK_SHOULD_NOT_BE_PRESENT = new WebSocketException(1002, "Message should not be masked");
	static final WebSocketException STATUS_CODE_MISSING = new WebSocketException(1005, "Status code missing");
	static final WebSocketException CLOSE_FRAME_MISSING = new WebSocketException(1006, "Peer did not send CLOSE frame");
	static final WebSocketException NOT_A_VALID_UTF_8 = new WebSocketException(1007, "Received TEXT message is not a valid UTF-8 message");
	static final WebSocketException MESSAGE_TOO_BIG = new WebSocketException(1009, "Received message is too big");
	static final WebSocketException SERVER_ERROR = new WebSocketException(1011, "Unexpected server error");

	static final HttpException HANDSHAKE_FAILED = new HttpException("Failed to perform a proper opening handshake");
	static final HttpError UPGRADE_WITH_BODY = HttpError.badRequest400("Upgrade request contained body");

	static final HttpError NOT_A_WEB_SOCKET_REQUEST = HttpError.ofCode(400, "Not a websocket request");
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

		static @Nullable WebSocketConstants.OpCode fromOpCodeByte(byte b) {
			for (OpCode value : OpCode.values()) {
				if (value.code == b) {
					return value;
				}
			}
			return null;
		}
	}
}
