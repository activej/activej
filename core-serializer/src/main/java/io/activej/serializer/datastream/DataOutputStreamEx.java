package io.activej.serializer.datastream;

import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class DataOutputStreamEx implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(DataOutputStreamEx.class);

	public static final IllegalStateException SIZE_EXCEPTION = new IllegalStateException("Size of data exceeds 2MB");
	public static final int DEFAULT_BUFFER_SIZE = 16384;

	private BinaryOutput out;
	private final OutputStream outputStream;

	private int estimatedDataSize = 1;

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

	private void ensureSize(int size) throws IOException {
		if (out.array().length - out.pos() < size) {
			doEnsureSize(size);
		}
	}

	private void doEnsureSize(int size) throws IOException {
		// flush previous values before resize
		doFlush();
		if (out.array().length - out.pos() < size) {
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
		serialize(serializer, value, 3);
	}

	public final <T> void serialize(BinarySerializer<T> serializer, T value, int headerSize) throws IOException {
		if (headerSize < 1 || headerSize > 3) {
			throw new IllegalArgumentException("Only header sizes 1, 2 and 3 are supported");
		}

		int positionBegin;
		int positionData;
		for (; ; ) {
			ensureSize(headerSize + estimatedDataSize);
			positionBegin = out.pos();
			positionData = positionBegin + headerSize;
			out.pos(positionData);
			try {
				serializer.encode(out, value);
			} catch (ArrayIndexOutOfBoundsException e) {
				int dataSize = out.array().length - positionData;
				out.pos(positionBegin);
				estimatedDataSize = dataSize + 1 + (dataSize >>> 1);
				continue;
			}
			break;
		}
		int positionEnd = out.pos();
		int dataSize = positionEnd - positionData;
		if (dataSize >= 1 << headerSize * 7) {
			headerSize = onUnderEstimate(headerSize, positionData, dataSize);
		}
		writeSize(out.array(), positionBegin, dataSize, headerSize);
		dataSize += dataSize >>> 2;
		if (dataSize > estimatedDataSize)
			estimatedDataSize = dataSize;
		else
			estimatedDataSize -= estimatedDataSize >>> 10;
	}

	private int onUnderEstimate(int headerSize, int positionData, int dataSize) {
		if (logger.isTraceEnabled()) {
			logger.trace("Underestimated data size to be less than {}, actual size was {} bytes",
					headerSize == 1 ? "128 bytes" : "16 KBytes", dataSize);
		}

		int nextHeaderSize = 1 + (31 - Integer.numberOfLeadingZeros(dataSize)) / 7;
		if (nextHeaderSize > 3) throw SIZE_EXCEPTION;
		int headerDelta = nextHeaderSize - headerSize;
		int newPositionData = positionData + headerDelta;
		int newPositionEnd = newPositionData + dataSize;

		try {
			System.arraycopy(out.array(), positionData, out.array(), newPositionData, dataSize);
		} catch (ArrayIndexOutOfBoundsException e) {
			// rare case when data overflows array
			byte[] oldArray = out.array();

			// ensured size without flush
			this.out = new BinaryOutput(allocate(newPositionEnd));
			System.arraycopy(oldArray, 0, out.array(), 0, positionData);
			System.arraycopy(oldArray, positionData, out.array(), newPositionData, dataSize);
			recycle(oldArray);
		}
		out.pos(newPositionEnd);
		return nextHeaderSize;
	}

	private static void writeSize(byte[] buf, int pos, int size, int headerSize) {
		if (headerSize == 1) {
			buf[pos] = (byte) size;
		} else {
			buf[pos] = (byte) (size | 0x80);
			size >>>= 7;

			if (headerSize == 2) {
				buf[pos + 1] = (byte) size;
			} else {
				buf[pos + 1] = (byte) (size | 0x80);
				size >>>= 7;

				assert headerSize == 3;

				buf[pos + 2] = (byte) size;
			}
		}
	}

	public final void write(byte[] b) throws IOException {
		ensureSize(b.length);
		out.write(b);
	}

	public final void write(byte[] b, int off, int len) throws IOException {
		ensureSize(len);
		out.write(b, off, len);
	}

	public final void writeBoolean(boolean v) throws IOException {
		ensureSize(1);
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
		ensureSize(2);
		out.writeShort(v);
	}

	public final void writeInt(int v) throws IOException {
		ensureSize(4);
		out.writeInt(v);
	}

	public final void writeLong(long v) throws IOException {
		ensureSize(8);
		out.writeLong(v);
	}

	public final void writeVarInt(int v) throws IOException {
		ensureSize(5);
		out.writeVarInt(v);
	}

	public final void writeVarLong(long v) throws IOException {
		ensureSize(9);
		out.writeVarLong(v);
	}

	public final void writeFloat(float v) throws IOException {
		ensureSize(4);
		out.writeFloat(v);
	}

	public final void writeDouble(double v) throws IOException {
		ensureSize(8);
		out.writeDouble(v);
	}

	public final void writeChar(char v) throws IOException {
		ensureSize(2);
		out.writeChar(v);
	}

	public final void writeString(@Nullable String s) throws IOException {
		writeUTF8Nullable(s);
	}

	public final void writeUTF8(@NotNull String s) throws IOException {
		ensureSize(5 + s.length() * 3);
		out.writeUTF8(s);
	}

	public final void writeIso88591(@NotNull String s) throws IOException {
		ensureSize(5 + s.length() * 3);
		out.writeIso88591(s);
	}

	public final void writeUTF16(@NotNull String s) throws IOException {
		ensureSize(5 + s.length() * 2);
		out.writeUTF16(s);
	}

	public final void writeUTF8Nullable(@Nullable String s) throws IOException {
		ensureSize(s != null ? 5 + s.length() * 3 : 1);
		out.writeUTF8Nullable(s);
	}

	public final void writeIso88591Nullable(@Nullable String s) throws IOException {
		ensureSize(s != null ? 5 + s.length() * 3 : 5 + 1);
		out.writeIso88591Nullable(s);
	}

	public final void writeUTF16Nullable(@Nullable String s) throws IOException {
		ensureSize(s != null ? 5 + s.length() * 2 : 5 + 1);
		out.writeUTF16Nullable(s);
	}

}
