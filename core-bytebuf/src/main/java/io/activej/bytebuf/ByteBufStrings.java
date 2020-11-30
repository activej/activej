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

package io.activej.bytebuf;

import io.activej.common.Checks;
import io.activej.common.concurrent.ThreadLocalCharArray;
import io.activej.common.exception.parse.ParseException;

import static io.activej.common.Checks.checkArgument;

/**
 * This class contains various fast string utilities for {@link ByteBuf ByteBufs} and byte arrays
 */
public final class ByteBufStrings {
	private static final boolean CHECK = Checks.isEnabled(ByteBufStrings.class);

	private static final ParseException READ_PAST_LIMIT = new ParseException(ByteBufStrings.class, "Malformed utf-8 input: Read past end");
	private static final ParseException READ_PAST_ARRAY_LENGTH = new ParseException(ByteBufStrings.class, "Malformed utf-8 input");

	public static final byte CR = (byte) '\r';
	public static final byte LF = (byte) '\n';
	public static final byte SP = (byte) ' ';
	public static final byte HT = (byte) '\t';

	/**
	 * Same as String.valueOf(Long.MIN_VALUE).getBytes()
	 */
	private static final byte[] MIN_LONG_BYTES = new byte[]{45, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 56};
	/**
	 * Same as String.valueOf(Integer.MIN_VALUE).getBytes()
	 */
	private static final byte[] MIN_INT_BYTES = new byte[]{45, 50, 49, 52, 55, 52, 56, 51, 54, 52, 56};

	// ASCII
	public static void encodeAscii(byte[] array, int pos, String string) {
		for (int i = 0; i < string.length(); i++) {
			array[pos++] = (byte) string.charAt(i);
		}
	}

	public static byte[] encodeAscii(String string) {
		byte[] array = new byte[string.length()];
		for (int i = 0; i < string.length(); i++) {
			array[i] = (byte) string.charAt(i);
		}
		return array;
	}

	public static void putAscii(ByteBuf buf, String string) {
		encodeAscii(buf.array(), buf.tail(), string);
		buf.moveTail(string.length());
	}

	public static ByteBuf wrapAscii(String string) {
		byte[] array = new byte[string.length()];
		for (int i = 0; i < string.length(); i++) {
			array[i] = (byte) string.charAt(i);
		}
		return ByteBuf.wrapForReading(array);
	}

	public static String decodeAscii(byte[] array, int pos, int len, char[] tmpBuffer) {
		if (CHECK) checkArgument(tmpBuffer.length >= len, "given char buffer is not big enough");
		int charIndex = 0, end = pos + len;
		while (pos < end) {
			int c = array[pos++] & 0xff;
			tmpBuffer[charIndex++] = (char) c;
		}
		return new String(tmpBuffer, 0, charIndex);
	}

	public static String decodeAscii(byte[] array, int pos, int len) {
		return decodeAscii(array, pos, len, ThreadLocalCharArray.ensure(len));
	}

	public static String asAscii(ByteBuf buf) {
		String str = decodeAscii(buf.array(), buf.head(), buf.readRemaining(), ThreadLocalCharArray.ensure(buf.readRemaining()));
		buf.recycle();
		return str;
	}

	public static String decodeAscii(byte[] array) {
		return decodeAscii(array, 0, array.length, ThreadLocalCharArray.ensure(array.length));
	}

	public static void toLowerCaseAscii(byte[] bytes, int pos, int len) {
		for (int i = pos; i < pos + len; i++) {
			byte b = bytes[i];
			if (b >= 'A' && b <= 'Z') {
				bytes[i] = (byte) (b + ((byte) 'a' - (byte) 'A'));
			}
		}
	}

	public static void toLowerCaseAscii(byte[] bytes) {
		toLowerCaseAscii(bytes, 0, bytes.length);
	}

	public static void toLowerCaseAscii(ByteBuf buf) {
		toLowerCaseAscii(buf.array(), buf.head(), buf.readRemaining());
	}

	public static void toUpperCaseAscii(byte[] bytes, int pos, int len) {
		for (int i = pos; i < pos + len; i++) {
			byte b = bytes[i];
			if (b >= 'a' && b <= 'z') {
				bytes[i] = (byte) (b + ((byte) 'A' - (byte) 'a'));
			}
		}
	}

	public static void toUpperCaseAscii(byte[] bytes) {
		toUpperCaseAscii(bytes, 0, bytes.length);
	}

	public static void toUpperCaseAscii(ByteBuf buf) {
		toUpperCaseAscii(buf.array(), buf.head(), buf.readRemaining());
	}

	public static boolean equalsLowerCaseAscii(byte[] lowerCasePattern, byte[] array, int offset, int size) {
		if (lowerCasePattern.length != size)
			return false;
		for (int i = 0; i < lowerCasePattern.length; i++) {
			byte p = lowerCasePattern[i];
			if (CHECK) checkArgument(p < 'A' || p > 'Z');
			byte a = array[offset + i];
			if (a >= 'A' && a <= 'Z')
				a += 'a' - 'A';
			if (a != p)
				return false;
		}
		return true;
	}

	public static boolean equalsUpperCaseAscii(byte[] upperCasePattern, byte[] array, int offset, int size) {
		if (upperCasePattern.length != size)
			return false;
		for (int i = 0; i < upperCasePattern.length; i++) {
			byte p = upperCasePattern[i];
			if (CHECK) checkArgument(p < 'a' || p > 'z');
			byte a = array[offset + i];
			if (a >= 'a' && a <= 'z')
				a += 'A' - 'z';
			if (a != p)
				return false;
		}
		return true;
	}

	public static int hashCode(byte[] array, int offset, int size) {
		int result = 1;
		for (int i = offset; i < offset + size; i++) {
			result = 31 * result + array[i];
		}
		return result;
	}

	public static int hashCode(byte[] array) {
		return hashCode(array, 0, array.length);
	}

	public static int hashCode(ByteBuf buf) {
		return hashCode(buf.array(), buf.head(), buf.readRemaining());
	}

	public static int hashCodeLowerCaseAscii(byte[] array, int offset, int size) {
		int result = 1;
		for (int i = offset; i < offset + size; i++) {
			byte a = array[i];
			if (a >= 'A' && a <= 'Z')
				a += 'a' - 'A';
			result = 31 * result + a;
		}
		return result;
	}

	public static int hashCodeLowerCaseAscii(byte[] array) {
		return hashCodeLowerCaseAscii(array, 0, array.length);
	}

	public static int hashCodeLowerCaseAscii(ByteBuf buf) {
		return hashCodeLowerCaseAscii(buf.array(), buf.head(), buf.readRemaining());
	}

	public static int hashCodeUpperCaseAscii(byte[] array, int offset, int size) {
		int result = 1;
		for (int i = offset; i < offset + size; i++) {
			byte a = array[i];
			if (a >= 'a' && a <= 'z')
				a += 'A' - 'a';
			result = 31 * result + a;
		}
		return result;
	}

	public static int hashCodeUpperCaseAscii(byte[] array) {
		return hashCodeUpperCaseAscii(array, 0, array.length);
	}

	public static int hashCodeUpperCaseAscii(ByteBuf buf) {
		return hashCodeUpperCaseAscii(buf.array(), buf.head(), buf.readRemaining());
	}

	// UTF-8
	public static int encodeUtf8(byte[] array, int pos, String string) {
		int p = pos;
		for (int i = 0; i < string.length(); i++) {
			p += encodeUtf8(array, p, string.charAt(i));
		}
		return p - pos;
	}

	public static int encodeUtf8(byte[] array, int pos, char c) {
		int p = pos;
		if (c <= 0x007F) {
			array[p++] = (byte) c;
		} else if (c > 0x07FF) {
			array[p++] = (byte) (0xE0 | c >> 12 & 0x0F);
			array[p++] = (byte) (0x80 | c >> 6 & 0x3F);
			array[p++] = (byte) (0x80 | c & 0x3F);
		} else {
			array[p++] = (byte) (0xC0 | c >> 6 & 0x1F);
			array[p++] = (byte) (0x80 | c & 0x3F);
		}
		return p - pos;
	}

	public static void putUtf8(ByteBuf buf, String string) {
		int size = encodeUtf8(buf.array(), buf.tail(), string);
		buf.moveTail(size);
	}

	public static void putUtf8(ByteBuf buf, char c) {
		int size = encodeUtf8(buf.array(), buf.tail(), c);
		buf.moveTail(size);
	}

	public static ByteBuf wrapUtf8(String string) {
		ByteBuf byteBuffer = ByteBufPool.allocate(string.length() * 3);
		int size = encodeUtf8(byteBuffer.array(), 0, string);
		byteBuffer.moveTail(size);
		return byteBuffer;
	}

	public static int decodeUtf8(byte[] array, int pos, int len, char[] buffer, int to) throws ParseException {
		int end = pos + len;
		try {
			while (pos < end) {
				int c = array[pos++] & 0xff;
				switch ((c >> 4) & 0x0F) {
					case 0:
					case 1:
					case 2:
					case 3:
					case 4:
					case 5:
					case 6:
					case 7:
						buffer[to++] = (char) c;
						break;
					case 12:
					case 13:
						buffer[to++] = (char) ((c & 0x1F) << 6 | array[pos++] & 0x3F);
						break;
					case 14:
						buffer[to++] = (char) ((c & 0x0F) << 12 | (array[pos++] & 0x3F) << 6 | (array[pos++] & 0x3F));
						break;
				}
			}
			if (pos > end) throw READ_PAST_LIMIT;
		} catch (ArrayIndexOutOfBoundsException ignored) {
			throw READ_PAST_ARRAY_LENGTH;
		}
		return to;
	}

	public static String decodeUtf8(byte[] array, int pos, int len, char[] tmpBuffer) throws ParseException {
		int charIndex = 0;
		charIndex = decodeUtf8(array, pos, len, tmpBuffer, charIndex);
		return new String(tmpBuffer, 0, charIndex);
	}

	public static String decodeUtf8(byte[] array, int pos, int len) throws ParseException {
		return decodeUtf8(array, pos, len, ThreadLocalCharArray.ensure(len));
	}

	public static String decodeUtf8(ByteBuf buf, char[] tmpBuffer) throws ParseException {
		return decodeUtf8(buf.array(), buf.head(), buf.readRemaining(), tmpBuffer);
	}

	public static String decodeUtf8(ByteBuf buf) throws ParseException {
		return decodeUtf8(buf.array(), buf.head(), buf.readRemaining(), new char[buf.readRemaining()]);
	}

	public static String decodeUtf8(byte[] array) throws ParseException {
		return decodeUtf8(array, 0, array.length, new char[array.length]);
	}

	public static String asUtf8(ByteBuf buf) throws ParseException {
		String str = decodeUtf8(buf.array(), buf.head(), buf.readRemaining(), ThreadLocalCharArray.ensure(buf.readRemaining()));
		buf.recycle();
		return str;
	}

	public static int encodeInt(byte[] array, int pos, int value) {
		if (value >= 0) {
			return encodePositiveInt(array, pos, value);
		} else {
			return encodeNegativeInt(array, pos, value);
		}
	}

	public static int encodePositiveInt(byte[] array, int pos, int value) {
		int digits = digitsInt(value);
		for (int i = pos + digits - 1; i >= pos; i--) {
			long digit = value % 10;
			value = value / 10;
			array[i] = (byte) ('0' + digit);
		}
		return digits;
	}

	private static int encodeNegativeInt(byte[] array, int pos, int value) {
		if (value == Integer.MIN_VALUE) {
			System.arraycopy(MIN_INT_BYTES, 0, array, pos, 11);
			return 11;
		}
		value = -value;
		int digits = digitsInt(value);
		int i = pos + digits;
		for (; i >= pos + 1; i--) {
			int digit = value % 10;
			value = value / 10;
			array[i] = (byte) ('0' + digit);
		}
		array[i] = (byte) '-';
		return digits + 1;
	}

	public static int encodeLong(byte[] array, int pos, long value) {
		if (value >= 0) {
			return encodePositiveLong(array, pos, value);
		} else {
			return encodeNegativeLong(array, pos, value);
		}
	}

	public static int encodePositiveLong(byte[] array, int pos, long value) {
		int digits = digitsLong(value);
		for (int i = pos + digits - 1; i >= pos; i--) {
			long digit = value % 10;
			value = value / 10;
			array[i] = (byte) ('0' + digit);
		}
		return digits;
	}

	private static int encodeNegativeLong(byte[] array, int pos, long value) {
		if (value == Long.MIN_VALUE) {
			System.arraycopy(MIN_LONG_BYTES, 0, array, pos, 20);
			return 20;
		}
		value = -value;
		int digits = digitsLong(value);
		int i = pos + digits;
		for (; i >= pos + 1; i--) {
			long digit = value % 10;
			value = value / 10;
			array[i] = (byte) ('0' + digit);
		}
		array[i] = (byte) '-';
		return digits + 1;
	}

	public static void putInt(ByteBuf buf, int value) {
		int digits = encodeInt(buf.array(), buf.tail(), value);
		buf.moveTail(digits);
	}

	public static void putPositiveInt(ByteBuf buf, int value) {
		int digits = encodePositiveInt(buf.array(), buf.tail(), value);
		buf.moveTail(digits);
	}

	public static void putLong(ByteBuf buf, long value) {
		int digits = encodeLong(buf.array(), buf.tail(), value);
		buf.moveTail(digits);
	}

	public static void putPositiveLong(ByteBuf buf, long value) {
		int digits = encodePositiveLong(buf.array(), buf.tail(), value);
		buf.moveTail(digits);
	}

	public static ByteBuf wrapInt(int value) {
		ByteBuf buf = ByteBufPool.allocate(11);
		int digits = encodeInt(buf.array, 0, value);
		buf.moveTail(digits);
		return buf;
	}

	public static ByteBuf wrapLong(long value) {
		ByteBuf buf = ByteBufPool.allocate(20);
		int digits = encodeLong(buf.array, 0, value);
		buf.moveTail(digits);
		return buf;
	}

	public static int decodeInt(byte[] array, int pos, int len) throws ParseException {
		int result = 0;

		int i = pos;
		int negate = 0;
		if (array[i] == (byte) '-') {
			negate = 1;
			i++;
		}

		for (; i < pos + len; i++) {
			byte b = (byte) (array[i] - '0');
			if (b < 0 || b >= 10) {
				throw new ParseException("Not a decimal value: " + new String(array, pos, len));
			}
			result = b + result * 10;
			if (result < 0) {
				if (result == Integer.MIN_VALUE && (i + 1 == pos + len)) {
					return Integer.MIN_VALUE;
				}
				throw new ParseException("Bigger than max int value: " + new String(array, pos, len));
			}
		}
		return (result ^ -negate) + negate;
	}

	public static long decodeLong(byte[] array, int pos, int len) throws ParseException {
		long result = 0;

		int i = pos;
		int negate = 0;
		if (array[i] == (byte) '-') {
			negate = 1;
			i++;
		}

		for (; i < pos + len; i++) {
			byte b = (byte) (array[i] - '0');
			if (b < 0 || b >= 10) {
				throw new ParseException("Not a decimal value: " + new String(array, pos, len));
			}
			result = b + result * 10;
			if (result < 0) {
				if (result == Long.MIN_VALUE && (i + 1 == pos + len)) {
					return Long.MIN_VALUE;
				}
				throw new ParseException("Bigger than max long value: " + new String(array, pos, len));
			}
		}
		return (result ^ -negate) + negate;
	}

	public static int decodePositiveInt(byte[] array, int pos, int len) throws ParseException {
		int result = 0;
		for (int i = pos; i < pos + len; i++) {
			byte b = (byte) (array[i] - '0');
			if (b < 0 || b >= 10) {
				throw new ParseException("Not a decimal value: " + new String(array, pos, len));
			}
			result = b + result * 10;
			if (result < 0) {
				throw new ParseException("Bigger than max int value: " + new String(array, pos, len));
			}
		}
		return result;
	}

	public static long decodePositiveLong(byte[] array, int pos, int len) throws ParseException {
		long result = 0;
		for (int i = pos; i < pos + len; i++) {
			byte b = (byte) (array[i] - '0');
			if (b < 0 || b >= 10) {
				throw new ParseException("Not a decimal value: " + new String(array, pos, len));
			}
			result = b + result * 10;
			if (result < 0) {
				throw new ParseException("Bigger than max long value: " + new String(array, pos, len));
			}
		}
		return result;
	}

	private static int digitsLong(long value) {
		long limit = 10;
		for (int i = 1; i <= 18; i++) {
			if (value < limit)
				return i;
			limit *= 10;
		}
		return 19;
	}

	private static int digitsInt(int value) {
		int limit = 10;
		for (int i = 1; i <= 9; i++) {
			if (value < limit)
				return i;
			limit *= 10;
		}
		return 10;
	}

}
