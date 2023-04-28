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
import io.activej.bytebuf.ByteBufs;
import io.activej.common.Checks;
import io.activej.common.MemSize;
import io.activej.common.initializer.WithInitializer;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.http.session.SessionServlet;
import io.activej.promise.Promisable;
import io.activej.promise.Promise;
import io.activej.types.TypeT;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.*;

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.nullify;
import static io.activej.csp.consumer.ChannelConsumers.recycling;

/**
 * Represents any HTTP message. Its internal byte buffers will be automatically recycled in HTTP client or HTTP server.
 */
@SuppressWarnings({"unused", "WeakerAccess", "PointlessBitwiseExpression"})
public abstract class HttpMessage {
	private static final boolean CHECKS = Checks.isEnabled(HttpMessage.class);

	/**
	 * This flag means that the body of this message should not be streamed
	 * and should be collected into a single body {@link ByteBuf}.
	 * This flag is removed when body is taken away or recycled.
	 */
	static final byte MUST_LOAD_BODY = 1 << 0;
	/**
	 * This flag means that the DEFLATE compression algorithm will be used
	 * to compress/decompress the body of this message.
	 */
	static final byte USE_GZIP = 1 << 1;

	/**
	 * This flag means that the body was already recycled and is not accessible.
	 * It is mostly used in assertions.
	 */
	static final byte RECYCLED = (byte) (1 << 7);

	private final HttpVersion version;

	@MagicConstant(flags = {MUST_LOAD_BODY, USE_GZIP, RECYCLED})
	byte flags;

	final HttpHeadersMultimap<HttpHeader, HttpHeaderValue> headers = new HttpHeadersMultimap<>();
	@Nullable ByteBuf body;
	@Nullable ChannelSupplier<ByteBuf> bodyStream;

	protected int maxBodySize;
	protected Map<Object, Object> attachments;

	protected HttpMessage(HttpVersion version) {
		this.version = version;
	}

	public HttpVersion getVersion() {
		return version;
	}

	@SuppressWarnings("unchecked")
	public abstract class Builder<B extends Builder<B, T>, T extends HttpMessage>
			implements io.activej.common.builder.Builder<T>, WithInitializer<B>, Promisable<T> {
		protected Builder() {}

		public final B withHeader(HttpHeader header, String string) {
			HttpHeaderValue headerValue = HttpHeaderValue.of(string);
			headers.add(header, headerValue);
			return (B) this;
		}

		public final B withHeader(HttpHeader header, byte[] value) {
			HttpHeaderValue headerValue = HttpHeaderValue.ofBytes(value, 0, value.length);
			headers.add(header, headerValue);
			return (B) this;
		}

		public final B withHeader(HttpHeader header, byte[] array, int off, int len) {
			HttpHeaderValue headerValue = HttpHeaderValue.ofBytes(array, off, len);
			headers.add(header, headerValue);
			return (B) this;
		}

		public final B withHeader(HttpHeader header, HttpHeaderValue value) {
			headers.add(header, value);
			return (B) this;
		}

		public final B withCookies(HttpCookie... cookies) {
			addCookies(List.of(cookies));
			return (B) this;
		}

		public final B withCookies(List<HttpCookie> cookies) {
			addCookies(cookies);
			return (B) this;
		}

		public final B withCookie(HttpCookie cookie) {
			addCookie(cookie);
			return (B) this;
		}

		protected abstract void addCookies(List<HttpCookie> cookies);

		protected abstract void addCookie(HttpCookie cookie);

		public final B withBodyStream(ChannelSupplier<ByteBuf> bodySupplier) {
			HttpMessage.this.bodyStream = bodySupplier;
			return (B) this;
		}

		public final B withBody(ByteBuf body) {
			HttpMessage.this.body = body;
			return (B) this;
		}

		public final B withBody(byte[] body) {
			return withBody(ByteBuf.wrapForReading(body));
		}

		public final B withMaxBodySize(MemSize maxBodySize) {
			HttpMessage.this.maxBodySize = maxBodySize.toInt();
			return (B) this;
		}

		public final B withMaxBodySize(int maxBodySize) {
			HttpMessage.this.maxBodySize = maxBodySize;
			return (B) this;
		}

		/**
		 * Sets this message to use the DEFLATE compression algorithm.
		 */
		public final B withBodyGzipCompression() {
			HttpMessage.this.flags |= USE_GZIP;
			return (B) this;
		}

		@Override
		public final Promise<T> promise() {
			return Promise.of(build());
		}

		@Override
		public final T build() {
			return (T) HttpMessage.this;
		}
	}

	public final Collection<Map.Entry<HttpHeader, HttpHeaderValue>> getHeaders() {
		if (CHECKS) checkState(!isRecycled());
		return headers.getEntries();
	}

	public final <T> List<T> getHeader(HttpHeader header, HttpHeaderValue.DecoderInToListStreamConsumer<T> decoder) {
		if (CHECKS) checkState(!isRecycled());
		List<T> list = new ArrayList<>();
		for (int i = header.hashCode() & (headers.kvPairs.length - 2); ; i = (i + 2) & (headers.kvPairs.length - 2)) {
			HttpHeader k = (HttpHeader) headers.kvPairs[i];
			if (k == null) {
				break;
			}
			if (k.equals(header)) {
				try {
					decoder.decode(((HttpHeaderValue) headers.kvPairs[i + 1]).getBuf(), list);
				} catch (MalformedHttpException ignored) {
				}
			}
		}
		return list;
	}

	public <T> @Nullable T getHeader(HttpHeader header, HttpDecoderFunction<T> decoder) {
		if (CHECKS) checkState(!isRecycled());
		try {
			ByteBuf buf = getHeaderBuf(header);
			if (buf != null) {
				return decoder.decode(buf);
			}
		} catch (MalformedHttpException ignore) {
		}

		return null;
	}

	public final @Nullable String getHeader(HttpHeader header) {
		if (CHECKS) checkState(!isRecycled());
		HttpHeaderValue headerValue = headers.get(header);
		return headerValue != null ? headerValue.toString() : null;
	}

	public final @Nullable ByteBuf getHeaderBuf(HttpHeader header) {
		if (CHECKS) checkState(!isRecycled());
		HttpHeaderValue headerBuf = headers.get(header);
		return headerBuf != null ? headerBuf.getBuf() : null;
	}

	/**
	 * This method transfers the "rust-like ownership" from this message object
	 * to the caller.
	 * Thus, it can be called only once, and it is the caller's responsibility
	 * to recycle the byte buffers received.
	 *
	 * @see #takeBodyStream()
	 * @deprecated as the {@link #takeBodyStream()} method has a more semantically correct name.
	 * Both methods are identical though
	 */
	@Deprecated
	public ChannelSupplier<ByteBuf> getBodyStream() {
		return takeBodyStream();
	}

	/**
	 * This method transfers the "rust-like ownership" from this message object
	 * to the caller.
	 * Thus, it can be called only once, and it is the caller's responsibility
	 * to recycle the byte buffers received.
	 */
	public ChannelSupplier<ByteBuf> takeBodyStream() {
		if (CHECKS) checkState(!isRecycled());
		ChannelSupplier<ByteBuf> bodyStream = this.bodyStream;
		this.bodyStream = null;
		if (bodyStream != null) return bodyStream;
		if (body != null) {
			ByteBuf body = this.body;
			this.body = null;
			return ChannelSuppliers.ofValue(body);
		}
		throw new IllegalStateException("Body stream is missing or already consumed");
	}

	/**
	 * Allows you to peak at the body when it is available without taking the ownership.
	 */
	public final ByteBuf getBody() {
		if (CHECKS) checkState(!isRecycled());
		if ((flags & MUST_LOAD_BODY) != 0) throw new IllegalStateException("Body is not loaded");
		if (body != null) return body;
		throw new IllegalStateException("Body is missing or already consumed");
	}

	/**
	 * Similarly to {@link #takeBodyStream()}, this method transfers ownership and can be called only once.
	 * It returns successfully only when this message is in {@link #MUST_LOAD_BODY non-streaming mode}
	 */
	public final ByteBuf takeBody() {
		if (CHECKS) checkState(!isRecycled());
		ByteBuf body = getBody();
		this.body = null;
		return body;
	}

	/**
	 * Checks if this message is working in streaming mode or not.
	 * Returns true if not.
	 */
	public final boolean isBodyLoaded() {
		return (flags & MUST_LOAD_BODY) == 0 && body != null;
	}

	/**
	 * @see #loadBody(int)
	 */
	public Promise<ByteBuf> loadBody() {
		if (CHECKS) checkState(!isRecycled());
		return loadBody(maxBodySize);
	}

	/**
	 * @see #loadBody(int)
	 */
	public Promise<ByteBuf> loadBody(MemSize maxBodySize) {
		if (CHECKS) checkState(!isRecycled());
		return loadBody(maxBodySize.toInt());
	}

	/**
	 * Consumes the body stream if this message works in {@link #MUST_LOAD_BODY streaming mode} and collects
	 * it to a single {@link ByteBuf} or just returns the body if message is not in streaming mode.
	 *
	 * @param maxBodySize max number of bytes to load from the stream, an exception is returned if exceeded.
	 */
	public Promise<ByteBuf> loadBody(int maxBodySize) {
		if (CHECKS) checkState(!isRecycled());
		if (body != null) {
			this.flags &= ~MUST_LOAD_BODY;
			return Promise.of(body);
		}
		ChannelSupplier<ByteBuf> bodyStream = this.bodyStream;
		if (bodyStream == null) throw new IllegalStateException("Body stream is missing or already consumed");
		this.bodyStream = null;
		return ChannelSupplier.collect(bodyStream,
						new ByteBufs(),
						(bufs, buf) -> {
							if (maxBodySize != 0 && bufs.hasRemainingBytes(maxBodySize)) {
								bufs.recycle();
								buf.recycle();
								throw new MalformedHttpException(
										"HTTP body size exceeds load limit " + maxBodySize);
							}
							bufs.add(buf);
						},
						ByteBufs::takeRemaining)
				.whenResult(body -> {
					assert !isRecycled();

					this.flags &= ~MUST_LOAD_BODY;
					this.body = body;
				});
	}

	/**
	 * Attaches an arbitrary object to this message by its type.
	 * This is used for context management.
	 * For example some {@link SessionServlet wrapper auth servlet} could
	 * add some kind of session data here.
	 */
	public <T> void attach(Type type, T extra) {
		if (CHECKS) checkState(!isRecycled());
		if (attachments == null) {
			attachments = new HashMap<>();
		}
		attachments.put(type, extra);
	}

	/**
	 * @see #attach(Type, Object)
	 */
	public <T> void attach(Class<T> type, T extra) {
		if (CHECKS) checkState(!isRecycled());
		if (attachments == null) {
			attachments = new HashMap<>();
		}
		attachments.put(type, extra);
	}

	/**
	 * @see #attach(Type, Object)
	 */
	public <T> void attach(TypeT<T> typeT, T extra) {
		if (CHECKS) checkState(!isRecycled());
		if (attachments == null) {
			attachments = new HashMap<>();
		}
		attachments.put(typeT.getAnnotatedType(), extra);
	}

	/**
	 * @see #attach(Type, Object)
	 */
	public void attach(Object extra) {
		if (CHECKS) checkState(!isRecycled());
		if (attachments == null) {
			attachments = new HashMap<>();
		}
		attachments.put(extra.getClass(), extra);
	}

	/**
	 * Attaches an arbitrary object to this message by string key.
	 * This is used for context management.
	 */
	public <T> void attach(String key, T extra) {
		if (CHECKS) checkState(!isRecycled());
		if (attachments == null) {
			attachments = new HashMap<>();
		}
		attachments.put(key, extra);
	}

	/**
	 * @see #attach(Type, Object)
	 */
	@SuppressWarnings("unchecked")
	public <T> T getAttachment(Class<T> type) {
		if (attachments == null) {
			return null;
		}
		Object res = attachments.get(type);
		return (T) res;
	}

	/**
	 * @see #attach(Type, Object)
	 */
	@SuppressWarnings("unchecked")
	public <T> T getAttachment(TypeT<T> typeT) {
		if (attachments == null) {
			return null;
		}
		Object res = attachments.get(typeT.getAnnotatedType());
		return (T) res;
	}

	/**
	 * @see #attach(Type, Object)
	 */
	@SuppressWarnings("unchecked")
	public <T> T getAttachment(Type type) {
		if (attachments == null) {
			return null;
		}
		Object res = attachments.get(type);
		return (T) res;
	}

	/**
	 * @see #attach(String, Object)
	 */
	@SuppressWarnings("unchecked")
	public <T> T getAttachment(String key) {
		if (attachments == null) {
			return null;
		}
		Object res = attachments.get(key);
		return (T) res;
	}

	/**
	 * Retrieves a set of all attachment keys for this HttpMessage
	 */
	public Set<Object> getAttachmentKeys() {
		return attachments != null ? attachments.keySet() : Set.of();
	}

	boolean isRecycled() {
		return (this.flags & RECYCLED) != 0;
	}

	/**
	 * Shows whether content length should be present when there is no message body nor body stream
	 */
	abstract boolean isContentLengthExpected();

	final void recycle() {
		if (isRecycled()) return;
		flags |= RECYCLED;
		if (body != null) {
			body.recycle();
		}
		if (bodyStream != null) {
			bodyStream.streamTo(recycling());
		}
	}

	void recycleBody() {
		body = nullify(body, ByteBuf::recycle);
		bodyStream = nullify(bodyStream, stream -> stream.streamTo(recycling()));
	}

	protected void writeHeaders(ByteBuf buf) {
		if (CHECKS) checkState(!isRecycled());
		byte[] array = buf.array();
		int offset = buf.tail();
		for (int i = 0; i < headers.kvPairs.length - 1; i += 2) {
			HttpHeader k = (HttpHeader) headers.kvPairs[i];
			if (k != null) {
				HttpHeaderValue v = (HttpHeaderValue) headers.kvPairs[i + 1];
				array[offset++] = CR;
				array[offset++] = LF;
				offset = k.writeTo(array, offset);
				array[offset++] = (byte) ':';
				array[offset++] = SP;
				offset = v.writeTo(array, offset);
			}
		}
		array[offset++] = CR;
		array[offset++] = LF;
		array[offset++] = CR;
		array[offset++] = LF;
		buf.tail(offset);
	}

	protected int estimateSize(int firstLineSize) {
		if (CHECKS) checkState(!isRecycled());
		int size = firstLineSize;
		// CR,LF,header,": ",value
		for (int i = 0; i < headers.kvPairs.length - 1; i += 2) {
			HttpHeader k = (HttpHeader) headers.kvPairs[i];
			if (k != null) {
				HttpHeaderValue v = (HttpHeaderValue) headers.kvPairs[i + 1];
				// CR,LF,header,": ",value
				size += 2 + k.size() + 2 + v.estimateSize();
			}
		}
		size += 4; // CR,LF,CR,LF
		return size;
	}

	protected abstract int estimateSize();

	protected abstract void writeTo(ByteBuf buf);

	public interface HttpDecoderFunction<T> {
		T decode(ByteBuf value) throws MalformedHttpException;
	}
}
