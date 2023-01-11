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

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.ApplicationSettings;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.recycle.Recyclable;
import io.activej.common.ref.Ref;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelConsumers;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.http.ByteBufsDecoder_Multipart.MultipartFrame;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.activej.bytebuf.ByteBufStrings.CR;
import static io.activej.common.MemSize.kilobytes;
import static io.activej.common.Utils.nullify;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;

/**
 * Util class that allows to decode some binary channel (mainly, the request body stream) into a channel of multipart frames.
 */
public final class ByteBufsDecoder_Multipart implements ByteBufsDecoder<MultipartFrame>, WithInitializer<ByteBufsDecoder_Multipart> {
	private static final int MAX_META_SIZE = ApplicationSettings.getMemSize(ByteBufsDecoder_Multipart.class, "maxMetaBuffer", kilobytes(4)).toInt();
	private static final ByteBufsDecoder<ByteBuf> OF_CRLF_DECODER = ByteBufsDecoder.ofCrlfTerminatedBytes();

	private @Nullable List<String> readingHeaders = null;

	private final byte[] boundary;
	private final byte[] lastBoundary;

	private ByteBufsDecoder_Multipart(String boundary) {
		this.boundary = ("--" + boundary).getBytes(UTF_8);
		this.lastBoundary = ("--" + boundary + "--").getBytes(UTF_8);
	}

	public static ByteBufsDecoder_Multipart create(String boundary) {
		return new ByteBufsDecoder_Multipart(boundary);
	}

	/**
	 * Converts resulting channel of frames into a binary channel, ignoring any multipart headers.
	 */
	public ByteBufsDecoder<ByteBuf> ignoreHeaders() {
		return bufs -> {
			MultipartFrame frame = tryDecode(bufs);
			if (frame == null || frame.isHeaders()) {
				return null;
			}
			return frame.getData();
		};
	}

	private Promise<Map<String, String>> getContentDispositionFields(MultipartFrame frame) {
		Map<String, String> headers = frame.getHeaders();
		assert headers != null;
		String header = headers.get("content-disposition");
		if (header == null) {
			return Promise.ofException(new MalformedHttpException("Headers had no Content-Disposition"));
		}
		String[] headerParts = header.split(";");
		if (headerParts.length == 0 || !"form-data".equals(headerParts[0].trim())) {
			return Promise.ofException(new MalformedHttpException("Content-Disposition type is not 'form-data'"));
		}
		return Promise.of(Arrays.stream(headerParts)
				.skip(1)
				.map(part -> part.trim().split("=", 2))
				.collect(toMap(s -> s[0], s -> {
					String value = s.length == 1 ? "" : s[1];
					// stripping double quotation
					return value.substring(1, value.length() - 1);
				})));
	}

	private Promise<Void> doSplit(MultipartFrame headerFrame, ChannelSupplier<MultipartFrame> frames,
			AsyncMultipartDataHandler dataHandler) {
		return getContentDispositionFields(headerFrame)
				.then(contentDispositionFields -> {
					String fieldName = contentDispositionFields.get("name");
					String fileName = contentDispositionFields.get("filename");
					Ref<MultipartFrame> lastRef = new Ref<>();
					return frames
							.until(f -> {
								if (f.isHeaders()) {
									lastRef.set(f);
									return true;
								}
								return false;
							})
							.filter(MultipartFrame::isData)
							.map(MultipartFrame::getData)
							.streamTo(ChannelConsumer.ofPromise(fileName == null ?
									dataHandler.handleField(fieldName) :
									dataHandler.handleFile(fieldName, fileName)
							))
							.then(() -> lastRef.get() != null ?
									doSplit(lastRef.get(), frames, dataHandler) :
									Promise.complete())
							.toVoid();
				});
	}

	/**
	 * Complex operation that streams this channel of multipart frames into multiple binary consumers,
	 * as specified by the Content-Disposition multipart header.
	 */
	public Promise<Void> split(ChannelSupplier<ByteBuf> source, AsyncMultipartDataHandler dataHandler) {
		ChannelSupplier<MultipartFrame> frames = BinaryChannelSupplier.of(source).decodeStream(this);
		return frames.get()
				.thenIfNonNull(frame -> {
					if (frame.isHeaders()) {
						return doSplit(frame, frames, dataHandler);
					}
					Exception e = new MalformedHttpException("First frame had no headers");
					frames.closeEx(e);
					return Promise.ofException(e);
				});
	}

	private boolean sawCrlf = true;
	private boolean finished = false;

	@Override
	public @Nullable MultipartFrame tryDecode(ByteBufs bufs) throws MalformedDataException {
		if (finished) {
			return null;
		}

		while (true) {
			ByteBuf buf = OF_CRLF_DECODER.tryDecode(bufs);

			if (buf == null) break;

			if (sawCrlf) {
				if (readingHeaders == null) {
					if (buf.isContentEqual(lastBoundary)) {
						finished = true;
						buf.recycle();
					} else if (buf.isContentEqual(boundary)) {
						buf.recycle();
						readingHeaders = new ArrayList<>();
					} else {
						return getFalseTermFrame(buf);
					}
				} else {
					if (buf.canRead()) {
						readingHeaders.add(buf.asString(UTF_8));
						continue;
					}
					sawCrlf = false;
					buf.recycle();
					List<String> readingHeaders = this.readingHeaders;
					this.readingHeaders = null;
					if (readingHeaders.isEmpty()) {
						break;
					}
					return MultipartFrame.of(readingHeaders.stream()
							.map(s -> s.split(":\\s?", 2))
							.collect(toMap(s -> s[0].toLowerCase(), s -> s[1])));
				}
			} else {
				sawCrlf = true;
				return MultipartFrame.of(buf);
			}
		}

		int remaining = bufs.remainingBytes();
		if (sawCrlf) {
			if (readingHeaders == null && cannotBeBoundary(bufs)) {
				sawCrlf = false;
				return getFalseTermFrame(bufs.takeRemaining());
			}
			if (remaining >= MAX_META_SIZE) {
				throw new InvalidSizeException("Header size exceeds max meta size");
			}
			return null;
		}
		int toTake = remaining == 0 ? 0 : remaining - (bufs.peekByte(remaining - 1) == CR ? 1 : 0);
		if (toTake == 0) {
			return null;
		}
		ByteBuf data = bufs.takeExactSize(toTake);
		return MultipartFrame.of(data);
	}

	private MultipartFrame getFalseTermFrame(ByteBuf term) {
		ByteBuf buf = ByteBufPool.allocate(term.readRemaining() + 2);
		buf.writeByte((byte) '\r');
		buf.writeByte((byte) '\n');
		term.drainTo(buf, term.readRemaining());
		term.recycle();
		return MultipartFrame.of(buf);
	}

	private boolean cannotBeBoundary(ByteBufs bufs) throws MalformedDataException {
		return bufs.scanBytes((index, nextByte) -> {
			if (index == lastBoundary.length) {
				return nextByte != CR;
			} else if (index == lastBoundary.length - 1) {
				return nextByte != '-';
			} else if (index == boundary.length) {
				return nextByte != '-' && nextByte != CR;
			} else {
				assert index < boundary.length;
				return nextByte != boundary[index];
			}
		}) != 0;
	}

	public static final class MultipartFrame implements Recyclable {
		private @Nullable ByteBuf data;
		private final @Nullable Map<String, String> headers;

		private MultipartFrame(@Nullable ByteBuf data, @Nullable Map<String, String> headers) {
			this.data = data;
			this.headers = headers;
		}

		public static MultipartFrame of(ByteBuf data) {
			return new MultipartFrame(data, null);
		}

		public static MultipartFrame of(Map<String, String> headers) {
			return new MultipartFrame(null, headers);
		}

		public boolean isData() {
			return data != null;
		}

		public ByteBuf getData() {
			return data;
		}

		public boolean isHeaders() {
			return headers != null;
		}

		public Map<String, String> getHeaders() {
			return headers;
		}

		@Override
		public void recycle() {
			data = nullify(data, ByteBuf::recycle);
		}

		@Override
		public String toString() {
			return isHeaders() ? "headers" + headers : "" + data;
		}
	}

	public interface AsyncMultipartDataHandler {
		Promise<? extends ChannelConsumer<ByteBuf>> handleField(String fieldName);

		Promise<? extends ChannelConsumer<ByteBuf>> handleFile(String fieldName, String fileName);

		static AsyncMultipartDataHandler fieldsToMap(Map<String, String> fields) {
			return fieldsToMap(fields, ($1, $2) -> Promise.of(ChannelConsumers.recycling()));
		}

		static AsyncMultipartDataHandler fieldsToMap(Map<String, String> fields,
				Function<String, Promise<? extends ChannelConsumer<ByteBuf>>> uploader) {
			return fieldsToMap(fields, ($, fileName) -> uploader.apply(fileName));
		}

		static AsyncMultipartDataHandler fieldsToMap(Map<String, String> fields,
				BiFunction<String, String, Promise<? extends ChannelConsumer<ByteBuf>>> uploader) {
			return new AsyncMultipartDataHandler() {
				@Override
				public Promise<? extends ChannelConsumer<ByteBuf>> handleField(String fieldName) {
					return Promise.of(ChannelConsumer.ofSupplier(supplier -> supplier.toCollector(ByteBufs.collector())
							.map(value -> {
								fields.put(fieldName, value.asString(UTF_8));
								return null;
							})));
				}

				@Override
				public Promise<? extends ChannelConsumer<ByteBuf>> handleFile(String fieldName, String fileName) {
					return uploader.apply(fieldName, fileName);
				}
			};
		}

		static AsyncMultipartDataHandler file(Function<String, Promise<? extends ChannelConsumer<ByteBuf>>> uploader) {
			return files(($, fileName) -> uploader.apply(fileName));
		}

		static AsyncMultipartDataHandler files(BiFunction<String, String, Promise<? extends ChannelConsumer<ByteBuf>>> uploader) {
			return new AsyncMultipartDataHandler() {
				@Override
				public Promise<? extends ChannelConsumer<ByteBuf>> handleField(String fieldName) {
					return Promise.of(ChannelConsumers.recycling());
				}

				@Override
				public Promise<? extends ChannelConsumer<ByteBuf>> handleFile(String fieldName, String fileName) {
					return uploader.apply(fieldName, fileName);
				}
			};
		}

	}

}
