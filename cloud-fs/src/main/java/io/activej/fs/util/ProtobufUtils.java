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

package io.activej.fs.util;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.ByteBufsCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public final class ProtobufUtils {

	public static <I extends Message, O extends Message> ByteBufsCodec<I, O> codec(Parser<I> inputParser) {
		return new ByteBufsCodec<>() {
			@Override
			public ByteBuf encode(O item) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try {
					item.writeDelimitedTo(baos);
				} catch (IOException e) {
					throw new AssertionError(e);
				}
				return ByteBuf.wrapForReading(baos.toByteArray());
			}

			@Override
			public @Nullable I tryDecode(ByteBufs bufs) throws MalformedDataException {
				try {
					PeekingInputStream peekingInputStream = new PeekingInputStream(bufs);
					I result = inputParser.parseDelimitedFrom(peekingInputStream);
					bufs.skip(peekingInputStream.offset);
					return result;
				} catch (InvalidProtocolBufferException e) {
					IOException ioException = e.unwrapIOException();
					if (ioException == NEED_MORE_DATA_EXCEPTION) {
						return null;
					}
					throw new MalformedDataException(e);
				}
			}
		};
	}

	private static final NeedMoreDataException NEED_MORE_DATA_EXCEPTION = new NeedMoreDataException();

	private static final class NeedMoreDataException extends IOException {
		@Override
		public synchronized Throwable fillInStackTrace() {
			return this;
		}
	}

	private static class PeekingInputStream extends InputStream {
		private final ByteBufs bufs;
		int offset;

		public PeekingInputStream(ByteBufs bufs) {
			this.bufs = bufs;
			offset = 0;
		}

		@Override
		public int read() throws IOException {
			if (!bufs.hasRemainingBytes(offset + 1)) throw NEED_MORE_DATA_EXCEPTION;
			return bufs.peekByte(offset++);
		}

		@Override
		public int read(byte @NotNull [] b, int off, int len) throws IOException {
			if (!bufs.hasRemainingBytes(offset + 1)) throw NEED_MORE_DATA_EXCEPTION;

			int peeked = bufs.peekTo(this.offset, b, off, len);
			this.offset += peeked;
			return peeked;
		}
	}
}
