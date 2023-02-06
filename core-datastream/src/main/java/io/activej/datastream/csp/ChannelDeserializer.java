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
import io.activej.bytebuf.ByteBufs;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.TruncatedDataException;
import io.activej.common.exception.UnexpectedDataException;
import io.activej.common.exception.UnknownFormatException;
import io.activej.csp.ChannelInput;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.datastream.AbstractStreamSupplier;
import io.activej.datastream.StreamSupplier;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.lang.String.format;

/**
 * An adapter that converts a {@link ChannelSupplier} of {@link ByteBuf ByteBufs} to a {@link StreamSupplier} of some type,
 * that is deserialized from incoming binary data using given {@link BinarySerializer}.
 */
public final class ChannelDeserializer<T> extends AbstractStreamSupplier<T>
		implements WithChannelToStream<ChannelDeserializer<T>, ByteBuf, T> {
	private ChannelSupplier<ByteBuf> input;
	private final BinarySerializer<T> valueSerializer;

	private final ByteBufs bufs = new ByteBufs();

	private boolean explicitEndOfStream = false;

	private ChannelDeserializer(BinarySerializer<T> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	/**
	 * Creates a new instance of the deserializer for type T
	 */
	public static <T> ChannelDeserializer<T> create(BinarySerializer<T> valueSerializer) {
		return ChannelDeserializer.builder(valueSerializer).build();
	}

	/**
	 * Creates a builder of the deserializer for type T
	 */
	public static <T> ChannelDeserializer<T>.Builder builder(BinarySerializer<T> valueSerializer) {
		return new ChannelDeserializer<>(valueSerializer).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, ChannelDeserializer<T>> {
		private Builder() {}

		public Builder withExplicitEndOfStream() {
			checkNotBuilt(this);
			return withExplicitEndOfStream(true);
		}

		public Builder withExplicitEndOfStream(boolean explicitEndOfStream) {
			checkNotBuilt(this);
			ChannelDeserializer.this.explicitEndOfStream = explicitEndOfStream;
			return this;
		}

		@Override
		protected ChannelDeserializer<T> doBuild() {
			return ChannelDeserializer.this;
		}
	}

	@Override
	public ChannelInput<ByteBuf> getInput() {
		return input -> {
			checkInReactorThread(this);
			this.input = input;
			return getAcknowledgement();
		};
	}

	@Override
	protected void onResumed() {
		asyncBegin();

		final boolean endOfStream;

		try {
			endOfStream = process();
		} catch (CorruptedDataException e) {
			closeEx(new MalformedDataException("Data is corrupted", e));
			return;
		} catch (Exception e) {
			closeEx(new UnknownFormatException(format("Parse exception, %s : %s", this, bufs), e));
			return;
		}

		if (endOfStream) {
			assert bufs.hasRemainingBytes(1);
			bufs.skip(1);

			if (!explicitEndOfStream) {
				closeEx(new TruncatedDataException(format("Unexpected end-of-stream, %s : %s", this, bufs)));
				return;
			}

			if (bufs.hasRemaining()) {
				closeEx(new UnexpectedDataException(format("Unexpected data after end-of-stream, %s : %s", this, bufs)));
				return;
			}
		}

		if (isReady()) {
			input.get()
					.whenResult(buf -> {
						if (buf != null) {
							if (endOfStream) {
								buf.recycle();
								closeEx(new UnexpectedDataException(format("Unexpected data after end-of-stream, %s : %s", this, bufs)));
								return;
							}
							bufs.add(buf);
							asyncResume();
						} else {
							if (explicitEndOfStream && !endOfStream) {
								closeEx(new UnknownFormatException(format("Explicit end-of-stream is missing, %s : %s", this, bufs)));
								return;
							}

							if (bufs.isEmpty()) {
								sendEndOfStream();
							} else {
								closeEx(new TruncatedDataException(format("Truncated serialized data stream, %s : %s", this, bufs)));
							}
						}
					})
					.whenException(this::closeEx);
		} else {
			asyncEnd();
		}
	}

	private boolean process() {
		ByteBuf firstBuf;
		while (isReady() && (firstBuf = bufs.peekBuf()) != null) {
			int firstBufRemaining = firstBuf.readRemaining();
			if (firstBufRemaining >= 4) {
				byte[] array = firstBuf.array();
				int pos = firstBuf.head();
				byte b = array[pos];
				int messageSize;
				int headerSize;
				if (b > 0) {
					messageSize = b + 1;
					headerSize = 1;
				} else {
					int encodedSize = readEncodedSize(array, pos, b);
					if (encodedSize == 0) return true;
					headerSize = encodedSize >>> 28;
					messageSize = (encodedSize & 0x0FFFFFFF) + headerSize;
				}

				if (firstBufRemaining >= messageSize) {
					T item = valueSerializer.decode(array, pos + headerSize);
					send(item);
					if (firstBufRemaining != messageSize) {
						firstBuf.moveHead(messageSize);
					} else {
						bufs.take().recycle();
					}
					continue;
				}
			}

			int r = doProcess();
			if (r == 0) return true;
			if (r < 0) break;
		}

		return false;
	}

	private int doProcess() {
		int encodedSize = readEncodedSize();
		if (encodedSize == 0) return 0;
		int messageSize = encodedSize & 0x0FFFFFFF;
		int headerSize = encodedSize >>> 28;

		if (!bufs.hasRemainingBytes(messageSize)) {
			return -1;
		}

		bufs.consume(messageSize, buf -> {
			T item = valueSerializer.decode(buf.array(), buf.head() + headerSize);
			send(item);
		});

		return 1;
	}

	private static int readEncodedSize(byte[] array, int pos, byte b) {
		if (b < 0) {
			int dataSize = b & 0x7f;
			b = array[pos + 1];
			if (b >= 0) {
				dataSize += (b << 7);
				return dataSize + (2 << 28);
			} else {
				dataSize += ((b & 0x7f) << 7);
				b = array[pos + 2];
				if (b >= 0) {
					dataSize += (b << 14);
					return dataSize + (3 << 28);
				} else {
					dataSize += ((b & 0x7f) << 14);
					b = array[pos + 3];
					if (b >= 0) {
						dataSize += (b << 21);
						return dataSize + (4 << 28);
					}
					throw new CorruptedDataException("Invalid header size");
				}
			}
		}
		return 0;
	}

	private int readEncodedSize() {
		byte b = bufs.peekByte();
		if (b > 0) return b + 1 + (1 << 28);
		if (b == 0) return 0;
		if (bufs.hasRemainingBytes(2)) {
			int dataSize = b & 0x7f;
			b = bufs.peekByte(1);
			if (b >= 0) {
				dataSize += (b << 7);
				return dataSize + 2 + (2 << 28);
			}
			if (bufs.hasRemainingBytes(3)) {
				dataSize += ((b & 0x7f) << 7);
				b = bufs.peekByte(2);
				if (b >= 0) {
					dataSize += (b << 14);
					return dataSize + 3 + (3 << 28);
				}
				if (bufs.hasRemainingBytes(4)) {
					dataSize += ((b & 0x7f) << 14);
					b = bufs.peekByte(3);
					if (b >= 0) {
						dataSize += (b << 21);
						return dataSize + 4 + (4 << 28);
					}
					throw new CorruptedDataException("Invalid header size");
				}
			}
			return Integer.MAX_VALUE;
		}
		return Integer.MAX_VALUE;
	}

	@Override
	protected void onError(Exception e) {
		input.closeEx(e);
	}

	@Override
	protected void onCleanup() {
		bufs.recycle();
	}
}
