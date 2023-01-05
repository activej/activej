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
import io.activej.common.ApplicationSettings;
import io.activej.common.recycle.Recyclable;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.http.AsyncWebSocket.Frame.FrameType.CONTINUATION;
import static io.activej.http.AsyncWebSocket.Message.MessageType.BINARY;
import static io.activej.http.AsyncWebSocket.Message.MessageType.TEXT;

/**
 * Abstraction that allows to send and receive web socket data as frames or messages.
 * <p>
 * Since this interface extends {@link AsyncCloseable} interface, it is possible to close
 * a web socket with an appropriate exception. If an exception is an instance of {@link WebSocketException}
 * it will be translated to an appropriate close frame with corresponding close code and close reason.
 * <p>
 * Any other exception will result into close frame with close code {@code 1001} on a client
 * and {@code 1011} on a server.
 */
public interface AsyncWebSocket extends AsyncCloseable {
	boolean ENABLED = ApplicationSettings.getBoolean(AsyncWebSocket.class, "enabled", true);

	/**
	 * Returns a promise of a complete web socket message which consists of one or multiple data frames.
	 * <p>
	 * May return a promise of {@code null} if the other side has sent a close frame of code {@code 1000}
	 * in between other messages. A {@code null} message indicates end of stream.
	 * <p>
	 * All {@code readXXX} methods should be called serially. It is illegal to call this method after
	 * calling {@link #readFrame()} that did not return the last frame of a message.
	 *
	 * @return a complete web socket message or {@code null}
	 * @see Message
	 */
	Promise<Message> readMessage();

	/**
	 * Returns a promise of a web socket data frame. It may contain the whole web socket message or just some
	 * part of a message. Any UTF-8 validation for text frames should be done by the user of this method.
	 * <p>
	 * May return a promise of {@code null} if the other side has sent a close frame of code {@code 1000}.
	 * A {@code null} frame indicates end of stream.
	 * <p>
	 * All {@code readXXX} methods should be called serially. It is illegal to call {@link #readMessage()} after
	 * this method had been called and had not returned the last frame of a message.
	 *
	 * @return a web socket data frame or {@code null}
	 * @see Frame
	 */
	Promise<Frame> readFrame();

	/**
	 * A shortcut that allows to obtain a channel supplier of {@link Frame}s.
	 *
	 * @return a channel supplier of web socket data frames
	 * @see #readFrame()
	 * @see Frame
	 */
	default ChannelSupplier<Frame> frameReadChannel() {
		return ChannelSupplier.of(this::readFrame, this);
	}

	/**
	 * A shortcut that allows to obtain a channel supplier of {@link Message}s.
	 *
	 * @return a channel supplier of web socket messages
	 * @see #readMessage()
	 * @see Message
	 */
	default ChannelSupplier<Message> messageReadChannel() {
		return ChannelSupplier.of(this::readMessage, this);
	}

	/**
	 * A method for sending web socket messages.
	 * <p>
	 * If {@code null} is passed to this method, it indicates the end of stream. It is equivalent to sending a close frame
	 * with close code {@code 1000}, which indicates a normal closure of a web socket connection.
	 * <p>
	 * All {@code writeXXX} methods should be called serially. It is illegal to call this method after
	 * calling {@link #writeFrame(Frame)} with a frame that was not the last frame of a message.
	 *
	 * @param msg a web socket message to be sent
	 * @return a promise that indicates whether web socket message was successfully sent
	 */
	Promise<Void> writeMessage(@Nullable Message msg);

	/**
	 * A method for sending web socket data frames.
	 * <p>
	 * If {@code null} is passed to this method, it indicates the end of stream. It is equivalent to sending a close frame
	 * with close code {@code 1000}, which indicates a normal closure of a web socket connection.
	 * <p>
	 * All {@code writeXXX} methods should be called serially. It is illegal to call this method with
	 * frames that are out of order.
	 *
	 * @param frame a web socket data frame to be sent
	 * @return a promise that indicates whether data frame was successfully sent
	 */
	Promise<Void> writeFrame(@Nullable Frame frame);

	/**
	 * A shortcut that allows to obtain a channel consumer of {@link Frame}s.
	 *
	 * @return a channel consumer of web socket data frames
	 * @see #writeFrame(Frame)
	 * @see Frame
	 */
	default ChannelConsumer<Frame> frameWriteChannel() {
		return ChannelConsumer.of(this::writeFrame, this)
				.withAcknowledgement(ack -> ack.then(() -> writeFrame(null)));
	}

	/**
	 * A shortcut that allows to obtain a channel consumer of {@link Message}s.
	 *
	 * @return a channel consumer of web socket messages
	 * @see #writeMessage(Message)
	 * @see Message
	 */
	default ChannelConsumer<Message> messageWriteChannel() {
		return ChannelConsumer.of(this::writeMessage, this)
				.withAcknowledgement(ack -> ack
						.then(() -> writeMessage(null)));
	}

	/**
	 * A method for inspecting HTTP request associated with this web socket.
	 *
	 * @return HTTP request associated with this web socket
	 */
	HttpRequest getRequest();

	/**
	 * A method for inspecting HTTP response associated with this web socket.
	 *
	 * @return HTTP response associated with this web socket
	 */
	HttpResponse getResponse();

	/**
	 * Indicates whether this socket has already been closed or not
	 */
	boolean isClosed();

	/**
	 * Representation of a complete web socket message. It may contain either text or binary data.
	 */
	final class Message implements Recyclable {
		private final MessageType type;
		private final @Nullable ByteBuf binaryPayload;
		private final @Nullable String textPayload;

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

		public enum MessageType {
			TEXT, BINARY
		}
	}


	/**
	 * Representation of a web socket data frame. It may be one of text, binary or continuation types.
	 */
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

		public enum FrameType {
			TEXT, BINARY, CONTINUATION
		}
	}
}
