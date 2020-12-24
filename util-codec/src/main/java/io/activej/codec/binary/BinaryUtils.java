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

package io.activej.codec.binary;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.codec.StructuredDecoder;
import io.activej.codec.StructuredEncoder;
import io.activej.common.exception.MalformedDataException;

public final class BinaryUtils {

	public static <T> T decode(StructuredDecoder<T> decoder, byte[] bytes) throws MalformedDataException {
		return decode(decoder, ByteBuf.wrapForReading(bytes));
	}

	public static <T> T decode(StructuredDecoder<T> decoder, ByteBuf buf) throws MalformedDataException {
		try {
			BinaryStructuredInput in = new BinaryStructuredInput(buf);
			T result = decoder.decode(in);
			if (buf.readRemaining() != 0) {
				throw new MalformedDataException("Byte buffer was not fully consumed when decoding");
			}
			return result;
		} finally {
			buf.recycle();
		}
	}

	public static <T> ByteBuf encode(StructuredEncoder<T> encoder, T item) {
		BinaryStructuredOutput out = new BinaryStructuredOutput();
		encoder.encode(out, item);
		return out.getBuf();
	}

	public static <T> byte[] encodeAsArray(StructuredEncoder<T> encoder, T item) {
		return encode(encoder, item).asArray();
	}

	public static <T> void encodeInto(StructuredEncoder<T> encoder, T item, ByteBuf dest) {
		ByteBuf encoded = encode(encoder, item);
		dest.write(encoded.array(), encoded.head(), encoded.readRemaining());
	}

	public static <T> ByteBuf encodeWithSizePrefix(StructuredEncoder<T> encoder, T item) {
		BinaryStructuredOutput out = new BinaryStructuredOutput();
		encoder.encode(out, item);
		ByteBuf buf = ByteBufPool.allocate(out.getBuf().readRemaining() + 5);
		buf.writeVarInt(out.getBuf().readRemaining());
		buf.write(out.getBuf().array(), out.getBuf().head(), out.getBuf().readRemaining());
		out.getBuf().recycle();
		return buf;
	}
}
