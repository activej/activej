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
import io.activej.common.builder.AbstractBuilder;
import io.activej.csp.ChannelOutput;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.promise.Promise;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.function.BiConsumer;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.nullify;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.lang.Math.max;

/**
 * An adapter that converts a {@link ChannelConsumer} of {@link ByteBuf ByteBufs} to a {@link StreamConsumer} of some type,
 * that is serialized into binary data using given {@link BinarySerializer}.
 */
public final class ChannelSerializer<T> extends AbstractStreamConsumer<T>
		implements WithStreamToChannel<ChannelSerializer<T>, T, ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelSerializer.class);
	private static final boolean CHECKS = Checks.isEnabled(ChannelSerializer.class);

	/**
	 * Maximum allowed data size (256 MB).
	 * Messages with data whose size exceeds 256 MB are not supported
	 * <p>
	 * Because varlen encoding of data size is fully backward and forward compatible,
	 * even for smaller data it is possible to start with the smallest max data size (128 bytes),
	 * and fine-tune performance by switching to up to 4-byte encoding at later time.
	 */
	private static final int MAX_SIZE_INT = 1 << 28;

	public static final MemSize DEFAULT_INITIAL_BUFFER_SIZE = MemSize.kilobytes(16);

	private final BinarySerializer<T> serializer;

	private MemSize initialBufferSize = DEFAULT_INITIAL_BUFFER_SIZE;
	private byte @Nullable [] explicitEndOfStream;

	private @Nullable Duration autoFlushInterval;
	private BiConsumer<T, Exception> serializationErrorHandler = ($, e) -> closeEx(e);

	private Input input;
	private ChannelConsumer<ByteBuf> output;

	private final ArrayDeque<ByteBuf> bufs = new ArrayDeque<>();
	private boolean sending;

	private ChannelSerializer(BinarySerializer<T> serializer) {
		this.serializer = serializer;
	}

	/**
	 * Creates a new instance of the serializer for type T
	 */
	public static <T> ChannelSerializer<T> create(BinarySerializer<T> serializer) {
		return ChannelSerializer.builder(serializer).build();
	}

	/**
	 * Creates a builder of the serializer for type T
	 */
	public static <T> ChannelSerializer<T>.Builder builder(BinarySerializer<T> serializer) {
		return new ChannelSerializer<>(serializer).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, ChannelSerializer<T>> {
		private Builder() {}

		/**
		 * Sets the initial buffer size - a buffer of this size will
		 * be allocated first when trying to serialize incoming item
		 * <p>
		 * Defaults to 16kb
		 */
		public Builder withInitialBufferSize(MemSize bufferSize) {
			checkNotBuilt(this);
			ChannelSerializer.this.initialBufferSize = bufferSize;
			return this;
		}

		/**
		 * Sets the auto flush interval - when this is set the
		 * transformer will automatically flush itself at a given interval
		 */
		public Builder withAutoFlushInterval(@Nullable Duration autoFlushInterval) {
			checkNotBuilt(this);
			ChannelSerializer.this.autoFlushInterval = autoFlushInterval;
			return this;
		}

		/**
		 * Enables skipping of serialization errors.
		 * <p>
		 * When this method is called, the transformer ignores errors and just logs them,
		 * the default behaviour is closing serializer with the error.
		 */
		public Builder withSkipSerializationErrors() {
			checkNotBuilt(this);
			return withSerializationErrorHandler((item, e) -> logger.warn("Skipping serialization error for {} in {}", item, this, e));
		}

		/**
		 * Sets a serialization error handler for this serializer. Handler accepts serialized item and serialization error.
		 * The default handler simply closes serializer with received error.
		 */
		public Builder withSerializationErrorHandler(BiConsumer<T, Exception> handler) {
			checkNotBuilt(this);
			ChannelSerializer.this.serializationErrorHandler = handler;
			return this;
		}

		public Builder withExplicitEndOfStream() {
			checkNotBuilt(this);
			return withExplicitEndOfStream(true);
		}

		public Builder withExplicitEndOfStream(boolean explicitEndOfStream) {
			checkNotBuilt(this);
			return withExplicitEndOfStream(explicitEndOfStream ? new byte[]{0} : null);
		}

		public Builder withExplicitEndOfStream(byte @Nullable [] explicitEndOfStream) {
			checkNotBuilt(this);
			ChannelSerializer.this.explicitEndOfStream = explicitEndOfStream;
			return this;
		}

		@Override
		protected ChannelSerializer<T> doBuild() {
			return ChannelSerializer.this;
		}
	}

	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			checkInReactorThread(this);
			this.output = output;
			resume(input);
		};
	}

	@Override
	protected void onInit() {
		input = new Input(serializer, initialBufferSize.toInt(), serializationErrorHandler);
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
		send();
	}

	@Override
	protected void onError(Exception e) {
		output.closeEx(e);
	}

	@Override
	protected void onCleanup() {
		bufs.forEach(ByteBuf::recycle);
		bufs.clear();
		input.buf = nullify(input.buf, ByteBuf::recycle);
	}

	private void send() {
		if (sending) return;
		if (!bufs.isEmpty()) {
			sending = true;
			while (!bufs.isEmpty()) {
				Promise<Void> acceptPromise = output.accept(bufs.poll());
				if (acceptPromise.isResult()) continue;
				acceptPromise
						.run(($, e) -> {
							if (e == null) {
								sending = false;
								send();
							} else {
								closeEx(e);
							}
						});
				return;
			}
			sending = false;
			send();
		} else if (isEndOfStream()) {
			sending = true;
			Promise.complete()
					.then(() ->
							explicitEndOfStream != null ?
									output.accept(ByteBuf.wrapForReading(explicitEndOfStream)) :
									Promise.complete())
					.then(output::acceptEndOfStream)
					.whenResult(this::acknowledge)
					.whenException(this::closeEx);
		} else {
			resume(input);
		}
	}

	public final class Input implements StreamDataAcceptor<T> {
		private final BinarySerializer<T> serializer;

		private ByteBuf buf = null;
		private int estimatedDataSize;
		private int estimatedHeaderSize;
		private int requiredRemainingSize;

		private final int initialBufferSize;

		private final int autoFlushIntervalMillis;
		private boolean flushPosted;
		private final BiConsumer<T, Exception> serializationErrorHandler;

		public Input(BinarySerializer<T> serializer, int initialBufferSize, BiConsumer<T, Exception> serializationErrorHandler) {
			this.serializer = serializer;
			this.initialBufferSize = initialBufferSize;
			this.autoFlushIntervalMillis = autoFlushInterval == null ? Integer.MAX_VALUE : (int) autoFlushInterval.toMillis();
			this.serializationErrorHandler = serializationErrorHandler;
		}

		@Override
		public void accept(T item) {
			int positionBegin;
			int positionData;
			int positionEnd;
			for (; ; ) {
				if (buf == null || buf.writeRemaining() < requiredRemainingSize) {
					ensureBuffer();
				}
				positionBegin = buf.tail();
				positionData = positionBegin + estimatedHeaderSize;
				try {
					positionEnd = serializer.encode(buf.array(), positionData, item);
				} catch (ArrayIndexOutOfBoundsException e) {
					enlargeBuffer();
					continue;
				} catch (Exception e) {
					onSerializationError(item, e);
					return;
				}
				break;
			}
			buf.tail(positionEnd);
			int dataSize = positionEnd - positionData;
			if (dataSize > estimatedDataSize) {
				reestimate(positionBegin, positionData, dataSize);
			}
			writeSize(buf.array(), positionBegin, dataSize);
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

		private void ensureBuffer() {
			flush();
			buf = ByteBufPool.allocate(max(initialBufferSize, requiredRemainingSize));
			if (!flushPosted) {
				postFlush();
			}
		}

		private void enlargeBuffer() {
			int writeRemaining = buf.writeRemaining();
			flush();
			buf = ByteBufPool.allocate(max(initialBufferSize, writeRemaining + (writeRemaining >>> 1) + 1));
		}

		private void reestimate(int positionBegin, int positionData, int dataSize) {
			if (CHECKS) checkArgument(dataSize < MAX_SIZE_INT, "Serialized data size exceeds 256MB");
			estimatedDataSize = dataSize;
			estimatedHeaderSize = varIntSize(estimatedDataSize);
			requiredRemainingSize = estimatedHeaderSize + estimatedDataSize + (estimatedDataSize >>> 2);
			ensureHeaderSize(positionBegin, positionData, dataSize);
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

		private void postFlush() {
			flushPosted = true;
			if (autoFlushIntervalMillis <= 0) {
				reactor.postLast(() -> {
					flushPosted = false;
					flush();
				});
			} else if (autoFlushIntervalMillis < Integer.MAX_VALUE) {
				reactor.delayBackground(autoFlushIntervalMillis, () -> {
					flushPosted = false;
					flush();
				});
			}
		}

		private void flush() {
			if (buf == null) return;
			if (buf.canRead()) {
				if (!bufs.isEmpty()) {
					suspend();
				}
				bufs.add(buf);
				buf = null;
				send();
			} else {
				buf.recycle();
				buf = null;
			}
		}

		private void onSerializationError(T item, Exception e) {
			serializationErrorHandler.accept(item, e);
		}
	}

	private static int varIntSize(int value) {
		return 1 + (31 - Integer.numberOfLeadingZeros(value)) / 7;
	}
}
