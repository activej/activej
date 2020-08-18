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

import io.activej.async.process.AsyncCloseable;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.recycle.Recyclable;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.http.WebSocketConstants.FrameType;
import io.activej.http.WebSocketConstants.MessageType;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.http.WebSocketConstants.FrameType.CONTINUATION;
import static io.activej.http.WebSocketConstants.MessageType.BINARY;
import static io.activej.http.WebSocketConstants.MessageType.TEXT;

public interface WebSocket extends AsyncCloseable {
	@NotNull
	Promise<@Nullable Message> readMessage();

	@NotNull
	Promise<@Nullable Frame> readFrame();

	@NotNull
	default ChannelSupplier<Frame> frameReadChannel() {
		return ChannelSupplier.of(this::readFrame, this);
	}

	@NotNull
	default ChannelSupplier<Message> messageReadChannel() {
		return ChannelSupplier.of(this::readMessage, this);
	}

	@NotNull
	Promise<Void> writeMessage(@Nullable Message msg);

	@NotNull
	Promise<Void> writeFrame(@Nullable Frame frame);

	@NotNull
	default ChannelConsumer<Frame> frameWriteChannel() {
		return ChannelConsumer.of(this::writeFrame, this)
				.withAcknowledgement(ack -> ack.then(() -> writeFrame(null)));
	}

	@NotNull
	default ChannelConsumer<Message> messageWriteChannel() {
		return ChannelConsumer.of(this::writeMessage, this)
				.withAcknowledgement(ack -> ack
						.then(() -> writeMessage(null)));
	}

	HttpRequest getRequest();

	HttpResponse getResponse();

	final class Message implements Recyclable {
		private final MessageType type;
		@Nullable
		private final ByteBuf binaryPayload;
		@Nullable
		private final String textPayload;

		Message(MessageType type, @Nullable ByteBuf binaryPayload, @Nullable String textPayload) {
			this.type = type;
			this.textPayload = textPayload;
			this.binaryPayload = binaryPayload;
		}

		public static Message text(String payload) {
			return new Message(TEXT, null, payload);
		}

		public static Message binary(ByteBuf payload) {
			return new Message(BINARY, payload, null);
		}

		public MessageType getType() {
			return type;
		}

		public ByteBuf getBuf() {
			return checkNotNull(binaryPayload);
		}

		public String getText() {
			return checkNotNull(textPayload);
		}

		@Override
		public void recycle() {
			if (binaryPayload != null) {
				binaryPayload.recycle();
			}
		}
	}

	final class Frame implements Recyclable {
		private final FrameType type;
		private final ByteBuf payload;
		private final boolean isLastFrame;

		Frame(FrameType type, ByteBuf payload, boolean isLastFrame) {
			this.type = type;
			this.payload = payload;
			this.isLastFrame = isLastFrame;
		}

		public static Frame text(ByteBuf buf) {
			return new Frame(FrameType.TEXT, buf, true);
		}

		public static Frame text(ByteBuf buf, boolean isLastFrame) {
			return new Frame(FrameType.TEXT, buf, isLastFrame);
		}

		public static Frame binary(ByteBuf buf) {
			return new Frame(FrameType.BINARY, buf, true);
		}

		public static Frame binary(ByteBuf buf, boolean isLastFrame) {
			return new Frame(FrameType.BINARY, buf, isLastFrame);
		}

		public static Frame next(ByteBuf buf, boolean isLastFrame) {
			return new Frame(CONTINUATION, buf, isLastFrame);
		}

		public FrameType getType() {
			return type;
		}

		public ByteBuf getPayload() {
			return payload;
		}

		public boolean isLastFrame() {
			return isLastFrame;
		}

		@Override
		public void recycle() {
			payload.recycle();
		}
	}
}
