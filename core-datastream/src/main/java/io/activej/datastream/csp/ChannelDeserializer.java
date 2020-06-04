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
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.MemSize;
import io.activej.common.exception.parse.TruncatedDataException;
import io.activej.common.exception.parse.UnknownFormatException;
import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelSupplier;
import io.activej.datastream.AbstractStreamSupplier;
import io.activej.datastream.StreamSupplier;
import io.activej.serializer.BinarySerializer;

import static java.lang.String.format;

/**
 * An adapter that converts a {@link ChannelSupplier} of {@link ByteBuf ByteBufs} to a {@link StreamSupplier} of some type,
 * that is deserialized from incoming binary data using given {@link BinarySerializer}.
 */
public final class ChannelDeserializer<T> extends AbstractStreamSupplier<T> implements WithChannelToStream<ChannelDeserializer<T>, ByteBuf, T> {
	private ChannelSupplier<ByteBuf> input;
	private final BinarySerializer<T> valueSerializer;

	private final ByteBufQueue queue = new ByteBufQueue();

	private MemSize maxMessageSize = ChannelSerializer.MAX_SIZE;
	private boolean explicitEndOfStream = false;

	private ChannelDeserializer(BinarySerializer<T> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	/**
	 * Creates a new instance of the deserializer for type T
	 */
	public static <T> ChannelDeserializer<T> create(BinarySerializer<T> valueSerializer) {
		return new ChannelDeserializer<>(valueSerializer);
	}

	public ChannelDeserializer<T> withMaxMessageSize(MemSize maxMessageSize) {
		this.maxMessageSize = maxMessageSize;
		return this;
	}

	public ChannelDeserializer<T> withExplicitEndOfStream() {
		return withExplicitEndOfStream(true);
	}

	public ChannelDeserializer<T> withExplicitEndOfStream(boolean explicitEndOfStream) {
		this.explicitEndOfStream = explicitEndOfStream;
		return this;
	}

	@Override
	public ChannelInput<ByteBuf> getInput() {
		return input -> {
			this.input = input;
			return getEndOfStream();
		};
	}

	@Override
	protected void onResumed() {
		asyncBegin();

		final boolean endOfStream;

		try {
			endOfStream = maxMessageSize.toInt() <= ChannelSerializer.MAX_SIZE_1.toInt() ?
					process1() :
					process3();
		} catch (Exception e) {
			closeEx(new UnknownFormatException(ChannelDeserializer.class, format("Parse exception, %s : %s", this, queue), e));
			return;
		}

		if (endOfStream) {
			assert queue.hasRemainingBytes(1);
			queue.skip(1);

			if (!explicitEndOfStream) {
				closeEx(new UnknownFormatException(ChannelDeserializer.class, format("Unexpected end-of-stream, %s : %s", this, queue)));
				return;
			}

			if (queue.hasRemaining()) {
				closeEx(new UnknownFormatException(ChannelDeserializer.class, format("Unexpected data after end-of-stream, %s : %s", this, queue)));
				return;
			}
		}

		if (isReady()) {
			input.get()
					.whenResult(buf -> {
						if (buf != null) {
							if (endOfStream) {
								buf.recycle();
								closeEx(new UnknownFormatException(ChannelDeserializer.class, format("Unexpected data after end-of-stream, %s : %s", this, queue)));
								return;
							}
							queue.add(buf);
							asyncResume();
						} else {
							if (explicitEndOfStream && !endOfStream) {
								closeEx(new UnknownFormatException(ChannelDeserializer.class, format("Explicit end-of-stream is missing, %s : %s", this, queue)));
								return;
							}

							if (queue.isEmpty()) {
								sendEndOfStream();
							} else {
								closeEx(new TruncatedDataException(ChannelDeserializer.class, format("Truncated serialized data stream, %s : %s", this, queue)));
							}
						}
					})
					.whenException(this::closeEx);
		} else {
			asyncEnd();
		}
	}

	private boolean process1() {
		ByteBuf firstBuf;
		while (isReady() && (firstBuf = queue.peekBuf()) != null) {
			int size;

			byte[] array = firstBuf.array();
			int pos = firstBuf.head();
			byte b = array[pos];
			if (b > 0) {
				size = 1 + b;
			} else if (b < 0) {
				throw new IllegalArgumentException("Invalid header size");
			} else {
				return true;
			}

			int firstBufRemaining = firstBuf.readRemaining();
			if (firstBufRemaining >= size) {
				T item = valueSerializer.decode(array, pos + 1);
				send(item);
				if (firstBufRemaining != size) {
					firstBuf.moveHead(size);
				} else {
					queue.take().recycle();
				}
				continue;
			}

			if (!queue.hasRemainingBytes(size))
				break;

			queue.consume(size, buf -> {
				T item = valueSerializer.decode(buf.array(), buf.head() + 1);
				send(item);
			});
		}

		return false;
	}

	private boolean process3() {
		ByteBuf firstBuf;
		while (isReady() && (firstBuf = queue.peekBuf()) != null) {
			int dataSize;
			int headerSize;
			int size;
			int firstBufRemaining = firstBuf.readRemaining();
			if (firstBufRemaining >= 3) {
				byte[] array = firstBuf.array();
				int pos = firstBuf.head();
				byte b = array[pos];
				if (b > 0) {
					dataSize = b;
					headerSize = 1;
				} else if (b < 0) {
					dataSize = b & 0x7f;
					b = array[pos + 1];
					if (b >= 0) {
						dataSize += (b << 7);
						headerSize = 2;
					} else {
						dataSize += ((b & 0x7f) << 7);
						b = array[pos + 2];
						if (b >= 0) {
							dataSize += (b << 14);
							headerSize = 3;
						} else {
							throw new IllegalArgumentException("Invalid header size");
						}
					}
				} else {
					return true;
				}
				size = headerSize + dataSize;

				if (firstBufRemaining >= size) {
					T item = valueSerializer.decode(array, pos + headerSize);
					send(item);
					if (firstBufRemaining != size) {
						firstBuf.moveHead(size);
					} else {
						queue.take().recycle();
					}
					continue;
				}

			} else {
				byte b = queue.peekByte();
				if (b >= 0) {
					if (b == 0) return true;
					dataSize = b;
					headerSize = 1;
				} else if (queue.hasRemainingBytes(2)) {
					dataSize = b & 0x7f;
					b = queue.peekByte(1);
					if (b >= 0) {
						dataSize += (b << 7);
						headerSize = 2;
					} else if (queue.hasRemainingBytes(3)) {
						dataSize += ((b & 0x7f) << 7);
						b = queue.peekByte(2);
						if (b >= 0) {
							dataSize += (b << 14);
							headerSize = 3;
						} else {
							throw new IllegalArgumentException("Invalid header size");
						}
					} else {
						break;
					}
				} else {
					break;
				}
				size = headerSize + dataSize;
			}

			if (!queue.hasRemainingBytes(size))
				break;

			queue.consume(size, buf -> {
				T item = valueSerializer.decode(buf.array(), buf.head() + headerSize);
				send(item);
			});
		}

		return false;
	}

	@Override
	protected void onError(Throwable e) {
		input.closeEx(e);
	}

	@Override
	protected void onCleanup() {
		queue.recycle();
	}
}
