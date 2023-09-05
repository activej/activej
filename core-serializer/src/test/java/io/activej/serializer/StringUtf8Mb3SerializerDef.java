package io.activej.serializer;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.def.AbstractSerializerDef;
import io.activej.serializer.def.SerializerDef;
import io.activej.serializer.def.SerializerDefWithNullable;
import org.jetbrains.annotations.Nullable;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.util.BinaryOutputUtils.writeVarInt;

@SuppressWarnings({"unused", "DeprecatedIsStillUsed"})
@Deprecated
public final class StringUtf8Mb3SerializerDef extends AbstractSerializerDef implements SerializerDefWithNullable {
	public final boolean nullable;

	@SuppressWarnings("unused") // used via reflection
	public StringUtf8Mb3SerializerDef() {
		this(false);
	}

	public StringUtf8Mb3SerializerDef(boolean nullable) {
		this.nullable = nullable;
	}

	@Override
	public StringUtf8Mb3SerializerDef ensureNullable(CompatibilityLevel compatibilityLevel) {
		return new StringUtf8Mb3SerializerDef(true);
	}

	@Override
	public Class<?> getEncodeType() {
		return String.class;
	}

	@Override
	public Expression encode(SerializerDef.StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return set(pos, () -> {
			Expression string = cast(value, String.class);
			return nullable ?
				staticCall(StringUtf8Mb3SerializerDef.class, "writeUTF8mb3Nullable", buf, pos, string) :
				staticCall(StringUtf8Mb3SerializerDef.class, "writeUTF8mb3", buf, pos, string);
		});
	}

	@Override
	public Expression decode(SerializerDef.StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return nullable ?
			staticCall(StringUtf8Mb3SerializerDef.class, "readUTF8mb3Nullable", in) :
			staticCall(StringUtf8Mb3SerializerDef.class, "readUTF8mb3", in);
	}

	public static int writeUTF8mb3(byte[] buf, int off, String s) {
		int length = s.length();
		off = writeVarInt(buf, off, length);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			if (c <= 0x007F) {
				buf[off++] = (byte) c;
			} else {
				off = writeMb3UtfChar(buf, off, c);
			}
		}
		return off;
	}

	public static int writeUTF8mb3Nullable(byte[] buf, int off, String s) {
		if (s == null) {
			buf[off] = (byte) 0;
			return off + 1;
		}
		int length = s.length();
		off = writeVarInt(buf, off, length + 1);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			if (c <= 0x007F) {
				buf[off++] = (byte) c;
			} else {
				off = writeMb3UtfChar(buf, off, c);
			}
		}
		return off;
	}

	private static int writeMb3UtfChar(byte[] buf, int off, int c) {
		if (c <= 0x07FF) {
			buf[off] = (byte) (0xC0 | c >>> 6);
			buf[off + 1] = (byte) (0x80 | c & 0x3F);
			return off + 2;
		} else {
			buf[off] = (byte) (0xE0 | c >>> 12);
			buf[off + 1] = (byte) (0x80 | c >> 6 & 0x3F);
			buf[off + 2] = (byte) (0x80 | c & 0x3F);
			return off + 3;
		}
	}

	public static String readUTF8mb3(BinaryInput in) {
		int length = in.readVarInt();
		if (length == 0) return "";
		if (length >= 40) return readUTF8mb3buf(in, length);
		char[] chars = new char[length];
		for (int i = 0; i < length; i++) {
			byte b = in.array[in.pos++];
			chars[i] = b >= 0 ?
				(char) b :
				readUTF8mb3Char(in, b);
		}
		return new String(chars, 0, length);
	}

	public static @Nullable String readUTF8mb3Nullable(BinaryInput in) {
		int length = in.readVarInt();
		if (length == 0) return null;
		length--;
		if (length == 0) return "";
		if (length >= 40) return readUTF8mb3buf(in, length);
		char[] chars = new char[length];
		for (int i = 0; i < length; i++) {
			byte b = in.array[in.pos++];
			chars[i] = b >= 0 ?
				(char) b :
				readUTF8mb3Char(in, b);
		}
		return new String(chars, 0, length);
	}

	private static char readUTF8mb3Char(BinaryInput in, byte b) {
		int c = b & 0xFF;
		if (c < 0xE0) {
			return (char) ((c & 0x1F) << 6 | in.array[in.pos++] & 0x3F);
		} else {
			return (char) ((c & 0x0F) << 12 | (in.array[in.pos++] & 0x3F) << 6 | (in.array[in.pos++] & 0x3F));
		}
	}

	private static String readUTF8mb3buf(BinaryInput in, int length) {
		char[] chars = BinaryInput.BUF.getAndSet(null);
		if (chars == null || chars.length < length) chars = new char[length + length / 4];
		for (int i = 0; i < length; i++) {
			byte b = in.array[in.pos++];
			chars[i] = b >= 0 ?
				(char) b :
				readUTF8mb3Char(in, b);
		}
		String s = new String(chars, 0, length);
		BinaryInput.BUF.lazySet(chars);
		return s;
	}
}
