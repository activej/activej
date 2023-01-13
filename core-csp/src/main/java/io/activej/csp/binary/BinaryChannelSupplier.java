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

import io.activej.async.function.AsyncRunnable;
import io.activej.async.process.AbstractAsyncCloseable;
import io.activej.async.process.AsyncCloseable;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.Checks;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.TruncatedDataException;
import io.activej.common.exception.UnexpectedDataException;
import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static io.activej.common.function.FunctionEx.identity;
import static io.activej.reactor.Reactive.checkInReactorThread;

public abstract class BinaryChannelSupplier extends AbstractAsyncCloseable {
	private static final boolean CHECK = Checks.isEnabled(BinaryChannelSupplier.class);

	protected final ByteBufs bufs;

	protected BinaryChannelSupplier(ByteBufs bufs) {
		this.bufs = bufs;
	}

	protected BinaryChannelSupplier() {
		this.bufs = new ByteBufs();
	}

	public ByteBufs getBufs() {
		return bufs;
	}

	public abstract Promise<Void> needMoreData();

	public abstract Promise<Void> endOfStream();

	public static BinaryChannelSupplier ofList(List<ByteBuf> iterable) {
		return of(ChannelSupplier.ofList(iterable));
	}

	public static BinaryChannelSupplier ofIterator(Iterator<ByteBuf> iterator) {
		return of(ChannelSupplier.ofIterator(iterator));
	}

	public static BinaryChannelSupplier of(ChannelSupplier<ByteBuf> input) {
		return new BinaryChannelSupplier() {
			@Override
			public Promise<Void> needMoreData() {
				return input.get()
						.map(buf -> {
							if (buf != null) {
								bufs.add(buf);
								return null;
							} else {
								throw new TruncatedDataException("Unexpected end-of-stream");
							}
						});
			}

			@Override
			public Promise<Void> endOfStream() {
				if (!bufs.isEmpty()) {
					bufs.recycle();
					Exception exception = new UnexpectedDataException("Unexpected data after end-of-stream");
					input.closeEx(exception);
					return Promise.ofException(exception);
				}
				return input.get()
						.whenResult(Objects::nonNull,
								buf -> {
									buf.recycle();
									Exception exception = new UnexpectedDataException("Unexpected data after end-of-stream");
									input.closeEx(exception);
									throw exception;
								})
						.toVoid();
			}

			@Override
			protected void onClosed(Exception e) {
				input.closeEx(e);
			}
		};
	}

	public static BinaryChannelSupplier ofProvidedBufs(ByteBufs bufs,
			AsyncRunnable get, AsyncRunnable complete, AsyncCloseable closeable) {
		return new BinaryChannelSupplier(bufs) {
			@Override
			public Promise<Void> needMoreData() {
				return get.run();
			}

			@Override
			public Promise<Void> endOfStream() {
				return complete.run();
			}

			@Override
			protected void onClosed(Exception e) {
				closeable.closeEx(e);
			}
		};
	}

	public final <T> Promise<T> decode(ByteBufsDecoder<T> decoder) {
		if (CHECK) checkInReactorThread(this);
		return doDecode(decoder, this);
	}

	private <T> Promise<T> doDecode(ByteBufsDecoder<T> decoder, AsyncCloseable closeable) {
		while (true) {
			if (!bufs.isEmpty()) {
				T result;
				try {
					result = decoder.tryDecode(bufs);
				} catch (MalformedDataException e) {
					closeEx(e);
					return Promise.ofException(e);
				}
				if (result != null) {
					return Promise.of(result);
				}
			}
			Promise<Void> moreDataPromise = needMoreData();
			if (moreDataPromise.isResult()) continue;
			return moreDataPromise
					.whenException(closeable::closeEx)
					.then(() -> doDecode(decoder, closeable));
		}
	}

	public final <T> Promise<T> decodeRemaining(ByteBufsDecoder<T> decoder) {
		if (CHECK) checkInReactorThread(this);
		return decode(decoder)
				.then(result -> {
					if (!bufs.isEmpty()) {
						Exception exception = new UnexpectedDataException("Unexpected data after end-of-stream");
						closeEx(exception);
						throw exception;
					}
					return endOfStream().map($ -> result);
				});
	}

	public final <T> ChannelSupplier<T> decodeStream(ByteBufsDecoder<T> decoder) {
		checkInReactorThread(this);
		return ChannelSupplier.of(
				() -> doDecode(decoder,
						AsyncCloseable.of(e -> {
							if (e instanceof TruncatedDataException && bufs.isEmpty()) return;
							closeEx(e);
						}))
						.map(identity(),
								e -> {
									if (e instanceof TruncatedDataException && bufs.isEmpty()) return null;
									throw e;
								}),
				this);
	}

	@SuppressWarnings("UnusedReturnValue")
	public Promise<Void> bindTo(BinaryChannelInput input) {
		checkInReactorThread(this);
		return input.set(this);
	}

	@Override
	protected void onCleanup() {
		bufs.recycle();
	}
}
