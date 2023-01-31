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

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.bytebuf.ByteBufs.ByteScanner;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.Utils.IntByteScanner;
import io.activej.csp.binary.Utils.VarIntByteScanner;
import org.jetbrains.annotations.Nullable;

import static io.activej.bytebuf.ByteBufStrings.CR;
import static io.activej.bytebuf.ByteBufStrings.LF;
import static io.activej.csp.binary.Utils.decodeUntilTerminatorByte;

@FunctionalInterface
public interface ByteBufsDecoder<T> {

	@Nullable T tryDecode(ByteBufs bufs) throws MalformedDataException;

	default <V> ByteBufsDecoder<V> andThen(DecoderFunction<? super T, ? extends V> after) {
		return bufs -> {
			T maybeResult = tryDecode(bufs);
			if (maybeResult == null) return null;
			return after.decode(maybeResult);
		};
	}

	static ByteBufsDecoder<byte[]> assertBytes(byte[] data) {
		return bufs -> bufs.consumeBytes((index, b) -> {
			if (b != data[index]) {
				throw new MalformedDataException("Array of bytes differs at index " + index +
						"[Expected: " + data[index] + ", actual: " + b + ']');
			}
			return index == data.length - 1;
		}) != 0 ? data : null;
	}

	static ByteBufsDecoder<ByteBuf> ofFixedSize(int length) {
		return bufs -> {
			if (!bufs.hasRemainingBytes(length)) return null;
			return bufs.takeExactSize(length);
		};
	}

	static ByteBufsDecoder<ByteBuf> ofNullTerminatedBytes() {
		return ofNullTerminatedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsDecoder<ByteBuf> ofNullTerminatedBytes(int maxSize) {
		return decodeUntilTerminatorByte((byte) 0, maxSize);
	}

	static ByteBufsDecoder<ByteBuf> ofCrTerminatedBytes() {
		return ofCrTerminatedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsDecoder<ByteBuf> ofCrTerminatedBytes(int maxSize) {
		return decodeUntilTerminatorByte(CR, maxSize);
	}

	static ByteBufsDecoder<ByteBuf> ofCrlfTerminatedBytes() {
		return ofCrlfTerminatedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsDecoder<ByteBuf> ofCrlfTerminatedBytes(int maxSize) {
		return bufs -> {
			int bytes = bufs.scanBytes(new ByteScanner() {
				boolean crFound;

				@Override
				public boolean consume(int index, byte b) throws MalformedDataException {
					if (crFound) {
						if (b == LF) {
							return true;
						}
						crFound = false;
					}
					if (index == maxSize - 1) {
						throw new MalformedDataException("No CRLF is found in " + maxSize + " bytes");
					}
					if (b == CR) {
						crFound = true;
					}
					return false;
				}
			});

			if (bytes == 0) return null;

			ByteBuf buf = bufs.takeExactSize(bytes);
			buf.moveTail(-2);
			return buf;
		};
	}

	static ByteBufsDecoder<ByteBuf> ofIntSizePrefixedBytes() {
		return ofIntSizePrefixedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsDecoder<ByteBuf> ofIntSizePrefixedBytes(int maxSize) {
		IntByteScanner scanner = new IntByteScanner();
		return bufs -> {
			if (bufs.scanBytes(scanner) == 0) return null;

			int size = scanner.getValue();

			if (size < 0) throw new InvalidSizeException("Invalid size of bytes to be read, should be greater than 0");
			if (size > maxSize) throw new InvalidSizeException("Size exceeds max size");

			if (!bufs.hasRemainingBytes(4 + size)) return null;
			bufs.skip(4);
			return bufs.takeExactSize(size);
		};
	}

	static ByteBufsDecoder<ByteBuf> ofShortSizePrefixedBytes() {
		return ofShortSizePrefixedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsDecoder<ByteBuf> ofShortSizePrefixedBytes(int maxSize) {
		return bufs -> {
			if (!bufs.hasRemainingBytes(2)) return null;
			int size = (bufs.peekByte(0) & 0xFF) << 8
					| (bufs.peekByte(1) & 0xFF);
			if (size > maxSize) throw new InvalidSizeException("Size exceeds max size");
			if (!bufs.hasRemainingBytes(2 + size)) return null;
			bufs.skip(2);
			return bufs.takeExactSize(size);
		};
	}

	static ByteBufsDecoder<ByteBuf> ofByteSizePrefixedBytes() {
		return ofByteSizePrefixedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsDecoder<ByteBuf> ofByteSizePrefixedBytes(int maxSize) {
		return bufs -> {
			if (!bufs.hasRemaining()) return null;
			int size = bufs.peekByte() & 0xFF;
			if (size > maxSize) throw new InvalidSizeException("Size exceeds max size");
			if (!bufs.hasRemainingBytes(1 + size)) return null;
			bufs.skip(1);
			return bufs.takeExactSize(size);
		};
	}

	static ByteBufsDecoder<ByteBuf> ofVarIntSizePrefixedBytes() {
		return ofVarIntSizePrefixedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsDecoder<ByteBuf> ofVarIntSizePrefixedBytes(int maxSize) {
		VarIntByteScanner scanner = new VarIntByteScanner();
		return bufs -> {
			int bytes = bufs.scanBytes(scanner);

			if (bytes == 0) return null;

			int size = scanner.result;

			if (size < 0) throw new InvalidSizeException("Invalid size of bytes to be read, should be greater than 0");
			if (size > maxSize) throw new InvalidSizeException("Size exceeds max size");

			if (!bufs.hasRemainingBytes(bytes + size)) return null;
			bufs.skip(bytes);
			return bufs.takeExactSize(size);
		};
	}

}
