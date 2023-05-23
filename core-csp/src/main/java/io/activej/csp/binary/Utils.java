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

package io.activej.csp.binary;

import io.activej.async.function.AsyncSupplier;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.ToIntFunction;

import static io.activej.common.Utils.nullify;
import static java.lang.Math.min;

public class Utils {

	/**
	 * Creates an {@link OutputStream} out of a {@link ChannelConsumer <ByteBuf>}.
	 * <p>
	 * Asynchronous operations are executed in a context of a specified {@link Reactor}
	 * <p>
	 * Passed {@link ChannelSupplier <ByteBuf>} will be closed once a resulting {@link OutputStream} is closed or
	 * in case an error occurs while reading data.
	 * <p>
	 * <b>{@link OutputStream}'s methods are blocking, so they should not be called from a reactor</b>
	 *
	 * @param reactor         a reactor that will execute asynchronous operations
	 * @param channelConsumer a {@link ChannelSupplier<ByteBuf>} that is transformed to an {@link OutputStream}
	 * @return an {@link OutputStream} out ouf a {@link ChannelSupplier<ByteBuf>}
	 */
	public static OutputStream channelConsumerAsOutputStream(Reactor reactor, ChannelConsumer<ByteBuf> channelConsumer) {
		return new OutputStream() {
			private boolean isClosed;

			@Override
			public void write(int b) throws IOException {
				write(new byte[]{(byte) b}, 0, 1);
			}

			@SuppressWarnings("NullableProblems")
			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				if (isClosed) {
					throw new IOException("Stream Closed");
				}
				submit(reactor,
					() -> channelConsumer.accept(ByteBuf.wrap(b, off, off + len)),
					this);
			}

			@Override
			public void close() throws IOException {
				if (isClosed) return;
				isClosed = true;
				submit(reactor,
					() -> channelConsumer.acceptEndOfStream()
						.whenComplete(channelConsumer::close),
					this);
			}
		};
	}

	/**
	 * Creates an {@link InputStream} out of a {@link ChannelSupplier<ByteBuf>}.
	 * <p>
	 * Asynchronous operations are executed in a context of a specified {@link Reactor}
	 * <p>
	 * Passed {@link ChannelSupplier<ByteBuf>} will be closed once a resulting {@link InputStream} is closed or
	 * in case an error occurs while reading data.
	 * <p>
	 * <b>{@link InputStream}'s methods are blocking, so they should not be called from reactor</b>
	 *
	 * @param reactor         a reactor that will execute asynchronous operations
	 * @param channelSupplier a {@link ChannelSupplier<ByteBuf>} that is transformed to an {@link InputStream}
	 * @return an {@link InputStream} out ouf a {@link ChannelSupplier<ByteBuf>}
	 */
	public static InputStream channelSupplierAsInputStream(Reactor reactor, ChannelSupplier<ByteBuf> channelSupplier) {
		return new InputStream() {
			private @Nullable ByteBuf current = null;
			private boolean isClosed;
			private boolean isEOS;

			@Override
			public int read() throws IOException {
				return doRead(ByteBuf::readByte);
			}

			@SuppressWarnings("NullableProblems")
			@Override
			public int read(byte[] b, int off, int len) throws IOException {
				return doRead(buf -> buf.read(b, off, min(buf.readRemaining(), len)));
			}

			private int doRead(ToIntFunction<ByteBuf> reader) throws IOException {
				if (isClosed) {
					throw new IOException("Stream Closed");
				}
				if (isEOS) return -1;
				ByteBuf peeked = current;
				if (peeked == null) {
					ByteBuf buf;
					do {
						buf = submit(reactor, channelSupplier::get, this);
						if (buf == null) {
							isEOS = true;
							return -1;
						}
					} while (!buf.canRead());
					peeked = buf;
				}
				int result = reader.applyAsInt(peeked);
				if (peeked.canRead()) {
					current = peeked;
				} else {
					current = null;
					peeked.recycle();
				}
				return result;
			}

			@Override
			public void close() throws IOException {
				if (isClosed) return;
				isClosed = true;
				current = nullify(current, ByteBuf::recycle);
				submit(reactor,
					() -> {
						channelSupplier.close();
						return Promise.complete();
					},
					this);
			}
		};
	}

	private static <T> T submit(Reactor reactor, AsyncSupplier<T> supplier, Closeable closeable) throws IOException {
		CompletableFuture<T> future = reactor.submit(supplier::get);
		try {
			return future.get();
		} catch (InterruptedException e) {
			closeable.close();
			Thread.currentThread().interrupt();
			throw new IOException(e);
		} catch (ExecutionException e) {
			closeable.close();
			Throwable cause = e.getCause();
			if (cause instanceof IOException) throw (IOException) cause;
			if (cause instanceof RuntimeException) throw (RuntimeException) cause;
			if (cause instanceof Exception) throw new IOException(cause);
			if (cause instanceof Error) throw (Error) cause;
			throw new RuntimeException(cause);
		}
	}

	public static class VarIntByteScanner implements ByteBufs.ByteScanner {
		private int result;

		@Override
		public boolean consume(int index, byte b) throws MalformedDataException {
			result = (index == 0 ? 0 : result) | (b & 0x7F) << index * 7;
			if ((b & 0x80) == 0) {
				return true;
			}
			if (index == 4) {
				throw new InvalidSizeException("VarInt is too long for a 32-bit integer");
			}
			return false;
		}

		public int getResult() {
			return result;
		}
	}

	public static class IntByteScanner implements ByteBufs.ByteScanner {
		private int value;

		@Override
		public boolean consume(int index, byte b) {
			value = value << 8 | b & 0xFF;
			return index == 3;
		}

		public int getValue() {
			return value;
		}
	}
}
