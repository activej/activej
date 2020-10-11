package io.activej.serializer.datastream;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import static java.lang.Math.max;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DataInputStreamEx implements Closeable {
	public static final int DEFAULT_BUFFER_SIZE = 16384;

	private BinaryInput in;
	private int tail;
	private final InputStream inputStream;

	private char[] charArray = new char[128];

	private DataInputStreamEx(InputStream inputStream, int initialBufferSize) {
		this.inputStream = inputStream;
		this.in = new BinaryInput(allocate(initialBufferSize));
	}

	public static DataInputStreamEx create(InputStream inputStream) {
		return new DataInputStreamEx(inputStream, DEFAULT_BUFFER_SIZE);
	}

	public static DataInputStreamEx create(InputStream inputStream, int bufferSize) {
		return new DataInputStreamEx(inputStream, bufferSize);
	}

	@Override
	public final void close() throws IOException {
		recycle();
		inputStream.close();
	}

	private void recycle() {
		if (in == null) return;
		recycle(in.array());
		in = null;
	}

	protected byte[] allocate(int size) {
		return new byte[size];
	}

	protected void recycle(byte[] array) {
	}

	public BinaryInput getBinaryInput() {
		return in;
	}

	public void ensure(int size) throws IOException {
		if (tail - in.pos() < size) {
			doEnsureRead(size);
		}
	}

	private void doEnsureRead(int size) throws IOException {
		try {
			while (tail - in.pos() < size) {
				ensureWriteRemaining(size);
				int bytesRead = inputStream.read(in.array(), tail, in.array().length - tail);
				if (bytesRead == -1) {
					close();
					throw new CorruptedDataException("Unexpected end of data");
				}
				tail += bytesRead;
			}
		} catch (IOException e) {
			recycle();
			throw e;
		}
	}

	private void ensureWriteRemaining(int size) {
		int writeRemaining = in.array().length - tail;
		if (writeRemaining < size) {
			int readRemaining = tail - in.pos;
			if (in.array.length - readRemaining >= size) {
				System.arraycopy(in.array(), in.pos(), in.array(), 0, readRemaining);
				tail = readRemaining;
				in.pos = 0;
			} else {
				byte[] bytes = allocate(max(in.array.length, tail - in.pos() + size));
				System.arraycopy(in.array(), in.pos(), bytes, 0, tail - in.pos());
				tail -= in.pos();
				in = new BinaryInput(bytes);
			}
		}
	}

	public final boolean isEndOfStream() throws IOException {
		if (tail != in.pos()) {
			return false;
		} else {
			ensureWriteRemaining(1);
			int bytesRead = inputStream.read(in.array(), tail, tail - in.pos());
			if (bytesRead == -1) {
				recycle();
				return true;
			}
			tail += bytesRead;
			return false;
		}
	}

	public final <T> T deserialize(BinarySerializer<T> serializer) throws IOException {
		int messageSize = readSize();

		ensure(messageSize);

		int oldPos = in.pos();
		try {
			T item = serializer.decode(in);
			if (in.pos() - oldPos != messageSize) {
				throw new CorruptedDataException("Deserialized size != parsed data size");
			}
			return item;
		} catch (CorruptedDataException e) {
			close();
			throw e;
		}
	}

	private int readSize() throws IOException {
		int result;
		byte b = readByte();
		if (b >= 0) {
			result = b;
		} else {
			result = b & 0x7f;
			if ((b = readByte()) >= 0) {
				result |= b << 7;
			} else {
				result |= (b & 0x7f) << 7;
				if ((b = readByte()) >= 0) {
					result |= b << 14;
				} else {
					result |= (b & 0x7f) << 14;
					if ((b = readByte()) >= 0) {
						result |= b << 21;
					} else {
						close();
						throw new CorruptedDataException("Invalid size");
					}
				}
			}
		}
		return result;
	}

	private char[] ensureCharArray(int length) {
		if (charArray.length < length) {
			charArray = new char[length + (length >>> 2)];
		}
		return charArray;
	}

	public final int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	public final int read(byte[] b, int off, int len) throws IOException {
		ensure(len);
		in.read(b, off, len);
		return len;
	}

	public final byte readByte() throws IOException {
		return in.pos < tail ? in.readByte() : readByteImpl();
	}

	private byte readByteImpl() throws IOException {
		doEnsureRead(1);
		return in.readByte();
	}

	public final boolean readBoolean() throws IOException {
		ensure(1);
		return in.readBoolean();
	}

	public final short readShort() throws IOException {
		ensure(2);
		return in.readShort();
	}

	public final int readInt() throws IOException {
		ensure(4);
		return in.readInt();
	}

	public final long readLong() throws IOException {
		ensure(8);
		return in.readLong();
	}

	public final int readVarInt() throws IOException {
		int result;
		byte b = readByte();
		if (b >= 0) {
			result = b;
		} else {
			result = b & 0x7f;
			if ((b = readByte()) >= 0) {
				result |= b << 7;
			} else {
				result |= (b & 0x7f) << 7;
				if ((b = readByte()) >= 0) {
					result |= b << 14;
				} else {
					result |= (b & 0x7f) << 14;
					if ((b = readByte()) >= 0) {
						result |= b << 21;
					} else {
						result |= (b & 0x7f) << 21;
						if ((b = readByte()) >= 0) {
							result |= b << 28;
						} else {
							close();
							throw new CorruptedDataException("VarInt value takes more than 5 bytes");
						}
					}
				}
			}
		}
		return result;
	}

	public final long readVarLong() throws IOException {
		long result = 0;
		for (int offset = 0; offset < 64; offset += 7) {
			byte b = readByte();
			result |= (long) (b & 0x7F) << offset;
			if ((b & 0x80) == 0)
				return result;
		}
		close();
		throw new CorruptedDataException("VarLong value takes more than 10 bytes");
	}

	public final float readFloat() throws IOException {
		ensure(4);
		return in.readFloat();
	}

	public final double readDouble() throws IOException {
		ensure(8);
		return in.readDouble();
	}

	public final char readChar() throws IOException {
		ensure(2);
		return in.readChar();
	}

	public final @NotNull String readString() throws IOException {
		return readUTF8();
	}

	public final @NotNull String readUTF8() throws IOException {
		int length = readVarInt();
		if (length == 0) return "";
		ensure(length);
		String str = new String(in.array(), in.pos(), length, UTF_8);
		in.pos(in.pos() + length);
		return str;
	}

	public final @NotNull String readIso88591() throws IOException {
		int length = readVarInt();
		if (length == 0) return "";
		ensure(length);

		char[] chars = ensureCharArray(length);
		for (int i = 0; i < length; i++) {
			int c = readByte() & 0xff;
			chars[i] = (char) c;
		}
		return new String(chars, 0, length);
	}

	public final @NotNull String readUTF16() throws IOException {
		int length = readVarInt();
		if (length == 0) return "";
		ensure(length * 2);

		char[] chars = ensureCharArray(length);
		for (int i = 0; i < length; i++) {
			byte b1 = in.array()[in.pos];
			byte b2 = in.array()[in.pos + 1];
			in.pos += 2;
			chars[i] = (char) (((b1 & 0xFF) << 8) + (b2 & 0xFF));
		}
		return new String(chars, 0, length);
	}

	public final @Nullable String readUTF8Nullable() throws IOException {
		int length = readVarInt();
		if (length-- == 0) return null;
		if (length == 0) return "";
		ensure(length);
		String str = new String(in.array(), in.pos(), length, UTF_8);
		in.pos(in.pos() + length);
		return str;
	}

	public final @Nullable String readIso88591Nullable() throws IOException {
		int length = readVarInt();
		if (length-- == 0) return null;
		if (length == 0) return "";
		ensure(length);

		char[] chars = ensureCharArray(length);
		for (int i = 0; i < length; i++) {
			int c = readByte() & 0xff;
			chars[i] = (char) c;
		}
		return new String(chars, 0, length);
	}

	public final @Nullable String readUTF16Nullable() throws IOException {
		int length = readVarInt();
		if (length-- == 0) return null;
		if (length == 0) return "";
		ensure(length * 2);

		char[] chars = ensureCharArray(length);
		for (int i = 0; i < length; i++) {
			byte b1 = in.array()[in.pos];
			byte b2 = in.array()[in.pos + 1];
			in.pos += 2;
			chars[i] = (char) (((b1 & 0xFF) << 8) + (b2 & 0xFF));
		}
		return new String(chars, 0, length);
	}

}
