package io.activej.serializer.stream;

import io.activej.common.initializer.WithInitializer;
import io.activej.serializer.BinaryOutput;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class StreamOutput implements Closeable, WithInitializer<StreamOutput> {
	public static final int DEFAULT_BUFFER_SIZE = 16384;

	private BinaryOutput out;
	private final OutputStream outputStream;

	private StreamOutput(OutputStream outputStream, int initialBufferSize) {
		this.outputStream = outputStream;
		this.out = new BinaryOutput(allocate(initialBufferSize));
	}

	public static StreamOutput create(OutputStream output) {
		return new StreamOutput(output, DEFAULT_BUFFER_SIZE);
	}

	public static StreamOutput create(OutputStream outputStream, int bufferSize) {
		return new StreamOutput(outputStream, bufferSize);
	}

	@Override
	public final void close() throws IOException {
		flush();
		outputStream.close();
		recycle(out.array());
		out = null;
	}

	public BinaryOutput out() {
		return out;
	}

	public void out(BinaryOutput out) {
		this.out = out;
	}

	public byte[] array() {
		return out.array();
	}

	public int limit() {
		return out.array().length;
	}

	public int remaining() {
		return limit() - out.pos();
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

	public final void write(byte[] b) throws IOException {
		if (remaining() >= b.length) {
			out.write(b);
		} else {
			doFlush();
			outputStream.write(b);
		}
	}

	public final void write(byte[] b, int off, int len) throws IOException {
		if (remaining() >= len) {
			out.write(b, off, len);
		} else {
			doFlush();
			outputStream.write(b, off, len);
		}
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
		ensure(10);
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

	public final void writeString(String s) throws IOException {
		writeUTF8(s);
	}

	public final void writeUTF8(String s) throws IOException {
		ensure(5 + s.length() * 3);
		out.writeUTF8(s);
	}

	public final void writeIso88591(String s) throws IOException {
		ensure(5 + s.length() * 3);
		out.writeIso88591(s);
	}

	public final void writeUTF16(String s) throws IOException {
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
