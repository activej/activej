package io.activej.ot.repository;

import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.JsonWriter.WriteObject;

import java.io.IOException;
import java.io.OutputStream;

import static com.dslplatform.json.JsonWriter.ESCAPE;
import static com.dslplatform.json.JsonWriter.QUOTE;

@SuppressWarnings("NullableProblems")
public class JsonIndentUtils {
	static final ThreadLocal<OnelineOutputStream> BYTE_STREAM = new ThreadLocal<>();

	public static <T> WriteObject<T> indent(WriteObject<T> writeObject) {
		if (writeObject instanceof IndentedWriteObject) return writeObject;
		return new IndentedWriteObject<>(writeObject);
	}

	public static <T> WriteObject<T> oneline(WriteObject<T> writeObject) {
		if (writeObject instanceof OnelinedWriteObject) return writeObject;
		return new OnelinedWriteObject<>(writeObject);
	}

	public static class IndentedWriteObject<T> implements WriteObject<T> {
		private final WriteObject<T> writeObject;

		public IndentedWriteObject(WriteObject<T> writeObject) {
			this.writeObject = writeObject;
		}

		@Override
		public void write(JsonWriter writer, T value) {
			OnelineOutputStream onelineOutputStream = BYTE_STREAM.get();
			if (!onelineOutputStream.enabled) {
				writeObject.write(writer, value);
			} else {
				writer.flush();
				onelineOutputStream.disable();
				writeObject.write(writer, value);
				writer.flush();
				onelineOutputStream.enable();
			}
		}
	}

	public static class OnelinedWriteObject<T> implements WriteObject<T> {
		private final WriteObject<T> writeObject;

		public OnelinedWriteObject(WriteObject<T> writeObject) {
			this.writeObject = writeObject;
		}

		@Override
		public void write(JsonWriter writer, T value) {
			OnelineOutputStream onelineOutputStream = BYTE_STREAM.get();
			if (onelineOutputStream.enabled) {
				writeObject.write(writer, value);
			} else {
				writer.flush();
				onelineOutputStream.enable();
				writeObject.write(writer, value);
				writer.flush();
				onelineOutputStream.disable();
			}
		}
	}

	public static final class OnelineOutputStream extends OutputStream {
		private final OutputStream out;

		private boolean inString = false;
		private boolean inEscape = false;

		private boolean firstWritten = false;
		private boolean enabled = false;

		OnelineOutputStream(OutputStream out) {
			this.out = out;
		}

		public void enable() {
			firstWritten = false;
			enabled = true;
		}

		public void disable() {
			enabled = false;
		}

		@Override
		public void write(int b) throws IOException {
			if (inString) {
				if (b == QUOTE && !inEscape) {
					inString = false;
				} else {
					inEscape = !inEscape && b == ESCAPE;
				}
				out.write(b);
			} else if (b == QUOTE) {
				inString = true;
				out.write(b);
				firstWritten = true;
			} else if (!Character.isWhitespace(b)) {
				firstWritten = true;
				out.write(b);
			} else {
				if (!enabled || !firstWritten) {
					out.write(b);
				}
			}
		}

		@Override
		public void write(byte[] bytes, int off, int len) throws IOException {
			int start = off;

			for (int i = off; i < off + len; i++) {
				int b = bytes[i];

				if (inString) {
					if (b == QUOTE && !inEscape) {
						inString = false;
					} else {
						inEscape = !inEscape && b == ESCAPE;
					}
				} else if (b == QUOTE) {
					inString = true;
					firstWritten = true;
				} else if (!Character.isWhitespace(b)) {
					firstWritten = true;
				} else {
					int length = i - start + (enabled && firstWritten ? 0 : 1);
					if (length != 0) {
						out.write(bytes, start, length);
					}
					start = i + 1;
				}
			}

			int remaining = off + len - start;
			if (remaining > 0) {
				out.write(bytes, start, remaining);
			}
		}
	}
}
