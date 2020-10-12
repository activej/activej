package io.activej.serializer.datastream;

import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class DataOutputStreamEx implements Closeable {
	private static final int MAX_SIZE = 1 << 28; // 256MB

	public static final int DEFAULT_BUFFER_SIZE = 16384;

	private BinaryOutput out;
	private final OutputStream outputStream;

	private int estimatedDataSize = 1;
	private int estimatedHeaderSize = 1;

	private DataOutputStreamEx(OutputStream outputStream, int initialBufferSize) {
		this.outputStream = outputStream;
		this.out = new BinaryOutput(allocate(initialBufferSize));
	}

	public static DataOutputStreamEx create(OutputStream output) {
		return new DataOutputStreamEx(output, DEFAULT_BUFFER_SIZE);
	}

	public static DataOutputStreamEx create(OutputStream outputStream, int bufferSize) {
		return new DataOutputStreamEx(outputStream, bufferSize);
	}

	@Override
	public final void close() throws IOException {
		flush();
		outputStream.close();
		recycle(out.array());
		out = null;
	}

	public BinaryOutput getBinaryOutput() {
		return out;
	}

	public byte[] array() {
		return out.array();
	}

	public int pos() {
		return out.pos();
	}

	public int limit() {
		return out.array().length;
	}

	public int remaining() {
		return limit() - pos();
	}

	public void ensure(int bytes) throws IOException {
		if (remaining() < bytes) {
			doEnsureSize(bytes);
		}
	}

	private void doEnsureSize(int size) throws IOException {
		// flush previous values before resize
		doFlush();
		if (remaining() < size) {
			recycle(out.array());
			this.out = new BinaryOutput(allocate(size));
		}
	}

	protected byte[] allocate(int size) {
		return new byte[size];
	}

	protected void recycle(byte[] array) {
	}

	public final void flush() throws IOException {
		doFlush();
		outputStream.flush();
	}

	private void doFlush() throws IOException {
		if (out.pos() > 0) {
			outputStream.write(out.array(), 0, out.pos());
			out.pos(0);
		}
	}

	public final <T> void serialize(BinarySerializer<T> serializer, T value) throws IOException {
		int positionBegin;
		int positionData;
		ensure(estimatedHeaderSize + estimatedDataSize + (estimatedDataSize >>> 2));
		for (; ; ) {
			positionBegin = out.pos();
			positionData = positionBegin + estimatedHeaderSize;
			out.pos(positionData);
			try {
				serializer.encode(out, value);
			} catch (ArrayIndexOutOfBoundsException e) {
				int dataSize = out.array().length - positionData;
				out.pos(positionBegin);
				ensure(estimatedHeaderSize + dataSize + 1 + (dataSize >>> 1));
				continue;
			}
			break;
		}

		int positionEnd = out.pos();
		int dataSize = positionEnd - positionData;
		if (dataSize > estimatedDataSize) {
			estimateMore(positionBegin, positionData, dataSize);
		}
		writeSize(out.array(), positionBegin, dataSize);
	}

	private void ensureHeaderSize(int positionBegin, int positionData, int dataSize) {
		int previousHeaderSize = positionData - positionBegin;
		if (previousHeaderSize == estimatedHeaderSize) return; // offset is enough for header

		int headerDelta = estimatedHeaderSize - previousHeaderSize;
		assert headerDelta > 0;
		int newPositionData = positionData + headerDelta;
		int newPositionEnd = newPositionData + dataSize;
		if (newPositionEnd < out.array().length) {
			System.arraycopy(out.array(), positionData, out.array(), newPositionData, dataSize);
		} else {
			// rare case when data overflows array
			byte[] oldArray = out.array();

			// ensured size without flush
			this.out = new BinaryOutput(allocate(newPositionEnd));
			System.arraycopy(oldArray, 0, out.array(), 0, positionBegin);
			System.arraycopy(oldArray, positionData, out.array(), newPositionData, dataSize);
			recycle(oldArray);
		}
		out.pos(newPositionEnd);
	}

	private void estimateMore(int positionBegin, int positionData, int dataSize) {
		assert dataSize < MAX_SIZE;

		estimatedDataSize = dataSize;
		estimatedHeaderSize = varIntSize(estimatedDataSize);
		ensureHeaderSize(positionBegin, positionData, dataSize);
	}

	private static int varIntSize(int dataSize) {
		return 1 + (31 - Integer.numberOfLeadingZeros(dataSize)) / 7;
	}

	private void writeSize(byte[] buf, int pos, int size) {
		if (estimatedHeaderSize == 1) {
			buf[pos] = (byte) size;
			return;
		}

		buf[pos] = (byte) ((size & 0x7F) | 0x80);
		size >>>= 7;
		if (estimatedHeaderSize == 2) {
			buf[pos + 1] = (byte) size;
			return;
		}

		buf[pos + 1] = (byte) ((size & 0x7F) | 0x80);
		size >>>= 7;
		if (estimatedHeaderSize == 3) {
			buf[pos + 2] = (byte) size;
			return;
		}

		assert estimatedHeaderSize == 4;
		buf[pos + 2] = (byte) ((size & 0x7F) | 0x80);
		size >>>= 7;
		buf[pos + 3] = (byte) size;
	}

	public final void write(byte[] b) throws IOException {
		ensure(b.length);
		out.write(b);
	}

	public final void write(byte[] b, int off, int len) throws IOException {
		ensure(len);
		out.write(b, off, len);
	}

	public final void writeBoolean(boolean v) throws IOException {
		ensure(1);
		out.writeBoolean(v);
	}

	public final void writeByte(byte v) throws IOException {
		if (out.pos() < out.array().length) {
			out.writeByte(v);
		} else {
			writeByteImpl(v);
		}
	}

	private void writeByteImpl(byte v) throws IOException {
		doEnsureSize(1);
		out.writeByte(v);
	}

	public final void writeShort(short v) throws IOException {
		ensure(2);
		out.writeShort(v);
	}

	public final void writeInt(int v) throws IOException {
		ensure(4);
		out.writeInt(v);
	}

	public final void writeLong(long v) throws IOException {
		ensure(8);
		out.writeLong(v);
	}

	public final void writeVarInt(int v) throws IOException {
		ensure(5);
		out.writeVarInt(v);
	}

	public final void writeVarLong(long v) throws IOException {
		ensure(9);
		out.writeVarLong(v);
	}

	public final void writeFloat(float v) throws IOException {
		ensure(4);
		out.writeFloat(v);
	}

	public final void writeDouble(double v) throws IOException {
		ensure(8);
		out.writeDouble(v);
	}

	public final void writeChar(char v) throws IOException {
		ensure(2);
		out.writeChar(v);
	}

	public final void writeString(@NotNull String s) throws IOException {
		writeUTF8(s);
	}

	public final void writeUTF8(@NotNull String s) throws IOException {
		ensure(5 + s.length() * 3);
		out.writeUTF8(s);
	}

	public final void writeIso88591(@NotNull String s) throws IOException {
		ensure(5 + s.length() * 3);
		out.writeIso88591(s);
	}

	public final void writeUTF16(@NotNull String s) throws IOException {
		ensure(5 + s.length() * 2);
		out.writeUTF16(s);
	}

	public final void writeUTF8Nullable(@Nullable String s) throws IOException {
		ensure(s != null ? 5 + s.length() * 3 : 1);
		out.writeUTF8Nullable(s);
	}

	public final void writeIso88591Nullable(@Nullable String s) throws IOException {
		ensure(s != null ? 5 + s.length() * 3 : 5 + 1);
		out.writeIso88591Nullable(s);
	}

	public final void writeUTF16Nullable(@Nullable String s) throws IOException {
		ensure(s != null ? 5 + s.length() * 2 : 5 + 1);
		out.writeUTF16Nullable(s);
	}
}
