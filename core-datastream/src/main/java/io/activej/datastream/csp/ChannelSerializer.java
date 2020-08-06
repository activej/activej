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

package io.activej.datastream.csp;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.MemSize;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelOutput;
import io.activej.datastream.AbstractStreamConsumer;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.promise.Promise;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.function.BiConsumer;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.nullify;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;
import static java.lang.Math.max;

/**
 * An adapter that converts a {@link ChannelConsumer} of {@link ByteBuf ByteBufs} to a {@link StreamConsumer} of some type,
 * that is serialized into binary data using given {@link BinarySerializer}.
 */
public final class ChannelSerializer<T> extends AbstractStreamConsumer<T> implements WithStreamToChannel<ChannelSerializer<T>, T, ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelSerializer.class);

	/**
	 * Binary format: 1-byte varlen message size + message, for messages with max size up to 128 bytes
	 * This is the most efficient and fast binary representation for both serializer and deserializer.
	 * <p>
	 * It is still possible to change max size at any time, switching from 1-byte to 2-byte or 3-byte header size or vice versa,
	 * because varlen encoding of message size is fully backward and forward compatible.
	 */
	public static final MemSize MAX_SIZE_1 = MemSize.bytes(128); // (1 << (1 * 7))

	/**
	 * Binary format: 2-byte varlen message size + message, for messages with max size up to 16KB
	 */
	public static final MemSize MAX_SIZE_2 = MemSize.kilobytes(16); // (1 << (2 * 7))

	/**
	 * Binary format: 3-byte varlen message size + message, for messages with max size up to 2MB
	 * Messages with size >2MB are not supported
	 */
	public static final MemSize MAX_SIZE_3 = MemSize.megabytes(2); // (1 << (3 * 7))

	/**
	 * Default setting for max message size (2MB).
	 * Messages with size >2MB are not supported
	 * <p>
	 * Because varlen encoding of message size is fully backward and forward compatible,
	 * even for smaller messages it is possible to start with default max message size (2MB),
	 * and fine-tune performance by switching to 1-byte or 2-byte encoding at later time.
	 */
	public static final MemSize MAX_SIZE = MAX_SIZE_3;

	private final BinarySerializer<T> serializer;

	private static final ArrayIndexOutOfBoundsException OUT_OF_BOUNDS_EXCEPTION = new ArrayIndexOutOfBoundsException("Message overflow");

	public static final MemSize DEFAULT_INITIAL_BUFFER_SIZE = MemSize.kilobytes(16);

	private MemSize initialBufferSize = DEFAULT_INITIAL_BUFFER_SIZE;
	private MemSize maxMessageSize = MAX_SIZE;
	private boolean explicitEndOfStream = false;

	@Nullable
	private Duration autoFlushInterval;
	private BiConsumer<T, Throwable> serializationErrorHandler = ($, e) -> closeEx(e);

	private Input input;
	private ChannelConsumer<ByteBuf> output;

	private final ArrayDeque<ByteBuf> bufs = new ArrayDeque<>();
	private boolean flushing;

	// region creators
	private ChannelSerializer(BinarySerializer<T> serializer) {
		this.serializer = serializer;
	}

	/**
	 * Creates a new instance of the serializer for type T
	 */
	public static <T> ChannelSerializer<T> create(BinarySerializer<T> serializer) {
		return new ChannelSerializer<>(serializer);
	}

	/**
	 * Sets the initial buffer size - a buffer of this size will
	 * be allocated first when trying to serialize incoming item
	 * <p>
	 * Defaults to 16kb
	 */
	public ChannelSerializer<T> withInitialBufferSize(MemSize bufferSize) {
		this.initialBufferSize = bufferSize;
		return this;
	}

	/**
	 * Sets the max message size - when a single message takes more
	 * than this amount of memory to be serialized, this transformer
	 * will be closed with {@link #OUT_OF_BOUNDS_EXCEPTION out of bounds excetion}
	 * unless {@link #withSkipSerializationErrors} was used to ignore such errors.
	 */
	public ChannelSerializer<T> withMaxMessageSize(MemSize maxMessageSize) {
		checkArgument(maxMessageSize.compareTo(MemSize.ZERO) > 0 && maxMessageSize.compareTo(MAX_SIZE_3) <= 0,
				"Maximum message size cannot be less than 0 bytes or larger than 2 megabytes");
		this.maxMessageSize = maxMessageSize;
		return this;
	}

	/**
	 * Sets the auto flush interval - when this is set the
	 * transformer will automatically flush itself at a given interval
	 */
	public ChannelSerializer<T> withAutoFlushInterval(@Nullable Duration autoFlushInterval) {
		this.autoFlushInterval = autoFlushInterval;
		return this;
	}

	/**
	 * Enables skipping of serialization errors.
	 * <p>
	 * When this method is called, the transformer ignores errors and just logs them,
	 * the default behaviour is closing serializer with the error.
	 */
	public ChannelSerializer<T> withSkipSerializationErrors() {
		return withSerializationErrorHandler((item, e) -> logger.warn("Skipping serialization error for {} in {}", item, this, e));
	}

	/**
	 * Sets a serialization error handler for this serializer. Handler accepts serialized item and serialization error.
	 * The default handler simply closes serializer with received error.
	 */
	public ChannelSerializer<T> withSerializationErrorHandler(BiConsumer<T, Throwable> handler) {
		this.serializationErrorHandler = handler;
		return this;
	}

	public ChannelSerializer<T> withExplicitEndOfStream() {
		return withExplicitEndOfStream(true);
	}

	public ChannelSerializer<T> withExplicitEndOfStream(boolean explicitEndOfStream) {
		this.explicitEndOfStream = explicitEndOfStream;
		return this;
	}

	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			this.output = output;
			resume(input);
		};
	}
	// endregion

	@Override
	protected void onInit() {
		input = new Input(serializer, initialBufferSize.toInt(), maxMessageSize.toInt(), autoFlushInterval, serializationErrorHandler);
	}

	@Override
	protected void onStarted() {
		if (output != null) {
			resume(input);
		}
	}

	@Override
	protected void onEndOfStream() {
		input.flush();
	}

	@Override
	protected void onError(Throwable e) {
		output.closeEx(e);
	}

	@Override
	protected void onCleanup() {
		bufs.forEach(ByteBuf::recycle);
		bufs.clear();
		input.buf = nullify(input.buf, ByteBuf::recycle);
	}

	private void doFlush() {
		if (flushing) return;
		if (!bufs.isEmpty()) {
			flushing = true;
			output.accept(bufs.poll())
					.whenResult(() -> {
						flushing = false;
						doFlush();
					})
					.whenException(this::closeEx);
		} else {
			if (isEndOfStream()) {
				flushing = true;
				Promise.complete()
						.then(() ->
								explicitEndOfStream ?
										output.accept(ByteBuf.wrapForReading(new byte[]{0})) :
										Promise.complete())
						.then(output::acceptEndOfStream)
						.whenResult(this::acknowledge);
			} else {
				resume(input);
			}
		}
	}

	private final class Input implements StreamDataAcceptor<T> {
		private final BinarySerializer<T> serializer;

		private ByteBuf buf = ByteBuf.empty();
		private int estimatedMessageSize;

		private final int headerSize;
		private final int maxMessageSize;
		private final int initialBufferSize;

		private final int autoFlushIntervalMillis;
		private boolean flushPosted;
		private final BiConsumer<T, Throwable> serializationErrorHandler;

		public Input(@NotNull BinarySerializer<T> serializer, int initialBufferSize, int maxMessageSize, @Nullable Duration autoFlushInterval, BiConsumer<T, Throwable> serializationErrorHandler) {
			this.serializationErrorHandler = serializationErrorHandler;
			this.serializer = serializer;
			this.maxMessageSize = maxMessageSize;
			this.headerSize = varIntSize(maxMessageSize - 1);
			this.estimatedMessageSize = 1;
			this.initialBufferSize = initialBufferSize;
			this.autoFlushIntervalMillis = autoFlushInterval == null ? -1 : (int) autoFlushInterval.toMillis();
		}

		@Override
		public void accept(T item) {
			int positionBegin;
			int positionItem;
			for (; ; ) {
				if (buf.writeRemaining() < headerSize + estimatedMessageSize + (estimatedMessageSize >>> 2)) {
					onFullBuffer();
				}
				positionBegin = buf.tail();
				positionItem = positionBegin + headerSize;
				buf.tail(positionItem);
				try {
					buf.tail(serializer.encode(buf.array(), buf.tail(), item));
				} catch (ArrayIndexOutOfBoundsException e) {
					onUnderEstimate(positionBegin);
					continue;
				} catch (Exception e) {
					onSerializationError(item, positionBegin, e);
					return;
				}
				break;
			}
			int positionEnd = buf.tail();
			int messageSize = positionEnd - positionItem;
			if (messageSize > estimatedMessageSize) {
				if (messageSize < maxMessageSize) {
					estimatedMessageSize = messageSize;
				} else {
					onSerializationError(item, positionBegin, OUT_OF_BOUNDS_EXCEPTION);
					return;
				}
			}
			writeSize(buf.array(), positionBegin, messageSize);
		}

		private void writeSize(byte[] buf, int pos, int size) {
			if (headerSize == 1) {
				buf[pos] = (byte) size;
				return;
			}

			buf[pos] = (byte) ((size & 0x7F) | 0x80);
			size >>>= 7;
			if (headerSize == 2) {
				buf[pos + 1] = (byte) size;
				return;
			}

			assert headerSize == 3;
			buf[pos + 1] = (byte) ((size & 0x7F) | 0x80);
			size >>>= 7;
			buf[pos + 2] = (byte) size;
		}

		private ByteBuf allocateBuffer() {
			return ByteBufPool.allocate(max(initialBufferSize, headerSize + estimatedMessageSize + (estimatedMessageSize >>> 2)));
		}

		private void onFullBuffer() {
			flush();
			buf = allocateBuffer();
			if (!flushPosted) {
				postFlush();
			}
		}

		private void onUnderEstimate(int positionBegin) {
			buf.tail(positionBegin);
			int writeRemaining = buf.writeRemaining();
			flush();
			buf = ByteBufPool.allocate(max(initialBufferSize, writeRemaining + (writeRemaining >>> 1) + 1));
		}

		private void onSerializationError(T item, int positionBegin, Exception e) {
			buf.tail(positionBegin);
			serializationErrorHandler.accept(item, e);
		}

		private void flush() {
			if (buf == null) return;
			if (buf.canRead()) {
				if (!bufs.isEmpty()) {
					suspend();
				}
				bufs.add(buf);
				estimatedMessageSize -= estimatedMessageSize >>> 8;
			} else {
				buf.recycle();
			}
			buf = ByteBuf.empty();
			doFlush();
		}

		private void postFlush() {
			flushPosted = true;
			if (autoFlushIntervalMillis == -1)
				return;
			if (autoFlushIntervalMillis == 0) {
				eventloop.postLast(wrapContext(this, () -> {
					flushPosted = false;
					flush();
				}));
			} else {
				eventloop.delayBackground(autoFlushIntervalMillis, wrapContext(this, () -> {
					flushPosted = false;
					flush();
				}));
			}
		}
	}

	private static int varIntSize(int value) {
		return 1 + (31 - Integer.numberOfLeadingZeros(value)) / 7;
	}
}
