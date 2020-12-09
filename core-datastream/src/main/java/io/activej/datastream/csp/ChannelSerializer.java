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
import io.activej.common.Checks;
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

import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.nullify;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;
import static java.lang.Math.max;

/**
 * An adapter that converts a {@link ChannelConsumer} of {@link ByteBuf ByteBufs} to a {@link StreamConsumer} of some type,
 * that is serialized into binary data using given {@link BinarySerializer}.
 */
public final class ChannelSerializer<T> extends AbstractStreamConsumer<T> implements WithStreamToChannel<ChannelSerializer<T>, T, ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelSerializer.class);
	private static final boolean CHECK = Checks.isEnabled(ChannelSerializer.class);

	/**
	 * Maximum allowed data size (256MB).
	 * Messages with data whose size exceeds 256MB are not supported
	 * <p>
	 * Because varlen encoding of data size is fully backward and forward compatible,
	 * even for smaller data it is possible to start with smallest max data size (128 bytes),
	 * and fine-tune performance by switching to up to 4-byte encoding at later time.
	 */
	private static final int MAX_SIZE_INT = 1 << 28;

	public static final MemSize DEFAULT_INITIAL_BUFFER_SIZE = MemSize.kilobytes(16);

	private final BinarySerializer<T> serializer;

	private MemSize initialBufferSize = DEFAULT_INITIAL_BUFFER_SIZE;
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
		input = new Input(serializer, initialBufferSize.toInt(), autoFlushInterval, serializationErrorHandler);
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
		private int estimatedDataSize;
		private int estimatedHeaderSize;

		private final int initialBufferSize;

		private final int autoFlushIntervalMillis;
		private boolean flushPosted;
		private final BiConsumer<T, Throwable> serializationErrorHandler;

		public Input(@NotNull BinarySerializer<T> serializer, int initialBufferSize, @Nullable Duration autoFlushInterval, BiConsumer<T, Throwable> serializationErrorHandler) {
			this.serializationErrorHandler = serializationErrorHandler;
			this.serializer = serializer;
			this.estimatedDataSize = 1;
			this.estimatedHeaderSize = 1;
			this.initialBufferSize = initialBufferSize;
			this.autoFlushIntervalMillis = autoFlushInterval == null ? -1 : (int) autoFlushInterval.toMillis();
		}

		@Override
		public void accept(T item) {
			int positionBegin;
			int positionData;
			for (; ; ) {
				if (buf.writeRemaining() < estimatedHeaderSize + estimatedDataSize + (estimatedDataSize >>> 2)) {
					onFullBuffer();
				}
				positionBegin = buf.tail();
				positionData = positionBegin + estimatedHeaderSize;
				buf.tail(positionData);
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
			int dataSize = positionEnd - positionData;
			if (dataSize > estimatedDataSize) {
				estimateMore(positionBegin, positionData, dataSize);
			}
			writeSize(buf.array(), positionBegin, dataSize);
		}

		private void estimateMore(int positionBegin, int positionData, int dataSize) {
			if (CHECK) checkState(dataSize < MAX_SIZE_INT, "Serialized data size exceeds 256MB");

			if (dataSize >= MAX_SIZE_INT) {
				throw new IllegalStateException("Size of data exceeds 256MB");
			}
			estimatedDataSize = dataSize;
			estimatedHeaderSize = varIntSize(estimatedDataSize);
			ensureHeaderSize(positionBegin, positionData, dataSize);
		}

		private void writeSize(byte[] buf, int pos, int size) {
			if (estimatedHeaderSize == 1) {
				buf[pos] = (byte) size;
				return;
			}

			buf[pos] = (byte) ((size & 0x7F) | 0x80);
			size >>>= 7;
			if (estimatedHeaderSize == 2) {
				buf[pos + 1] = (byte) size;
				return;
			}

			buf[pos + 1] = (byte) ((size & 0x7F) | 0x80);
			size >>>= 7;
			if (estimatedHeaderSize == 3) {
				buf[pos + 2] = (byte) size;
				return;
			}

			assert estimatedHeaderSize == 4;
			buf[pos + 2] = (byte) ((size & 0x7F) | 0x80);
			size >>>= 7;
			buf[pos + 3] = (byte) size;
		}

		private ByteBuf allocateBuffer() {
			return ByteBufPool.allocate(max(initialBufferSize, estimatedHeaderSize + estimatedDataSize + (estimatedDataSize >>> 2)));
		}

		private void ensureHeaderSize(int positionBegin, int positionData, int dataSize) {
			int previousHeaderSize = positionData - positionBegin;
			if (previousHeaderSize == estimatedHeaderSize) return; // offset is enough for header

			int headerDelta = estimatedHeaderSize - previousHeaderSize;
			assert headerDelta > 0;
			int newPositionData = positionData + headerDelta;
			int newPositionEnd = newPositionData + dataSize;
			if (newPositionEnd < buf.array().length) {
				System.arraycopy(buf.array(), positionData, buf.array(), newPositionData, dataSize);
			} else {
				// rare case when data overflows array
				ByteBuf old = buf;

				// ensured size without flush
				this.buf = ByteBufPool.allocate(max(initialBufferSize, newPositionEnd));
				System.arraycopy(old.array(), 0, buf.array(), 0, positionBegin);
				System.arraycopy(old.array(), positionData, buf.array(), newPositionData, dataSize);
				old.recycle();
			}
			buf.tail(newPositionEnd);
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
				estimatedDataSize -= estimatedDataSize >>> 8;
				estimatedHeaderSize = varIntSize(estimatedDataSize);
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
