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
import io.activej.bytebuf.ByteBufQueue;
import io.activej.bytebuf.ByteBufQueue.ByteScanner;
import io.activej.common.api.DecoderFunction;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.Utils.IntScanner;
import io.activej.csp.binary.Utils.VarIntScanner;
import org.jetbrains.annotations.Nullable;

import static io.activej.bytebuf.ByteBufStrings.CR;
import static io.activej.bytebuf.ByteBufStrings.LF;
import static io.activej.csp.binary.Utils.decodeUntilTerminatorByte;

@FunctionalInterface
public interface ByteBufsDecoder<T> {

	@Nullable
	T tryDecode(ByteBufQueue bufs) throws MalformedDataException;

	default <V> ByteBufsDecoder<V> andThen(DecoderFunction<? super T, ? extends V> after) {
		return bufs -> {
			T maybeResult = tryDecode(bufs);
			if (maybeResult == null) return null;
			return after.decode(maybeResult);
		};
	}

	static ByteBufsDecoder<byte[]> assertBytes(byte[] data) {
		return bufs ->
				bufs.decodeBytes((index, nextByte) -> {
					if (nextByte != data[index]) {
						throw new MalformedDataException("Array of bytes differs at index " + index +
								"[Expected: " + data[index] + ", actual: " + nextByte + ']');
					}
					return index == data.length - 1 ? data : null;
				});
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
			int lfIndex = bufs.scanBytes(new ByteScanner() {
				boolean crFound;

				@Override
				public boolean consume(int index, byte nextByte) throws MalformedDataException {
					if (crFound) {
						if (nextByte == LF) {
							return true;
						}
						crFound = false;
					}
					if (index == maxSize - 1) {
						throw new MalformedDataException("No CRLF is found in " + maxSize + " bytes");
					}
					if (nextByte == CR) {
						crFound = true;
					}
					return false;
				}
			});

			if (lfIndex == -1) return null;

			ByteBuf buf = bufs.takeExactSize(lfIndex - 1);
			bufs.skip(2);
			return buf;
		};
	}

	static ByteBufsDecoder<ByteBuf> ofIntSizePrefixedBytes() {
		return ofIntSizePrefixedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsDecoder<ByteBuf> ofIntSizePrefixedBytes(int maxSize) {
		return bufs -> {
			IntScanner scanner = new IntScanner();
			if (bufs.scanBytes(scanner) == -1) return null;

			int size = scanner.getResult();

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
		return bufs -> {
			VarIntScanner scanner = new VarIntScanner();
			int lastIndex = bufs.scanBytes(scanner);

			if (lastIndex == -1) return null;

			int size = scanner.getResult();

			if (size < 0) throw new InvalidSizeException("Invalid size of bytes to be read, should be greater than 0");
			if (size > maxSize) throw new InvalidSizeException("Size exceeds max size");

			int prefixSize = lastIndex + 1;
			if (!bufs.hasRemainingBytes(prefixSize + size)) return null;
			bufs.skip(prefixSize);
			return bufs.takeExactSize(size);
		};
	}

}
