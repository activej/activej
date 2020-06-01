package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.csp.AbstractChannelConsumer;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

import static io.activej.bytebuf.ByteBufStrings.decodeAscii;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestUtils {

	public static byte[] toByteArray(InputStream is) {
		byte[] bytes;

		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();

			byte[] data = new byte[1024];
			int count;

			while ((count = is.read(data)) != -1) {
				bos.write(data, 0, count);
			}

			bos.flush();
			bos.close();
			is.close();

			bytes = bos.toByteArray();
		} catch (IOException e) {
			throw new AssertionError(e);
		}
		return bytes;
	}

	public static void readFully(InputStream is, byte[] bytes) {
		DataInputStream dis = new DataInputStream(is);
		try {
			dis.readFully(bytes);
		} catch (IOException e) {
			throw new RuntimeException("Could not read " + new String(bytes, UTF_8), e);
		}
	}

	public static class AssertingConsumer extends AbstractChannelConsumer<ByteBuf> {
		private final ByteBufQueue queue = new ByteBufQueue();

		public boolean executed = false;
		@Nullable
		private byte[] expectedByteArray;
		@Nullable
		private String expectedString;
		@Nullable
		private ByteBuf expectedBuf;
		@Nullable
		private Exception expectedException;
		@Nullable
		private Consumer<Throwable> exceptionValidator;

		public void setExpectedByteArray(@Nullable byte[] expectedByteArray) {
			this.expectedByteArray = expectedByteArray;
		}

		public void setExpectedString(@Nullable String expectedString) {
			this.expectedString = expectedString;
		}

		public void setExpectedBuf(@Nullable ByteBuf expectedBuf) {
			this.expectedBuf = expectedBuf;
		}

		public void setExpectedException(@Nullable Exception expectedException) {
			this.expectedException = expectedException;
		}

		public void setExceptionValidator(@Nullable Consumer<Throwable> exceptionValidator) {
			this.exceptionValidator = exceptionValidator;
		}

		public void reset() {
			expectedBuf = null;
			expectedByteArray = null;
			expectedException = null;
			expectedString = null;
			exceptionValidator = null;
			executed = false;
		}

		@Override
		protected Promise<Void> doAccept(@Nullable ByteBuf value) {
			if (value != null) {
				queue.add(value);
			} else {
				ByteBuf actualBuf = queue.takeRemaining();

				try {
					if (expectedByteArray != null) {
						byte[] actualByteArray = actualBuf.getArray();
						assertArrayEquals(expectedByteArray, actualByteArray);
					}
					if (expectedString != null) {
						String actualString = decodeAscii(actualBuf.array(), actualBuf.head(), actualBuf.readRemaining());
						assertEquals(expectedString, actualString);
					}
					if (expectedBuf != null) {
						assertArrayEquals(expectedBuf.getArray(), actualBuf.getArray());
						expectedBuf.recycle();
						expectedBuf = null;
					}
				} finally {
					actualBuf.recycle();
				}

				executed = true;
			}
			return Promise.complete();
		}

		@Override
		protected void onCleanup() {
			queue.recycle();
			if (expectedBuf != null) {
				expectedBuf.recycle();
			}
		}

		@Override
		protected void onClosed(@NotNull Throwable e) {
			executed = true;
			if (expectedException != null) {
				assertEquals(expectedException, e);
				return;
			}
			if (exceptionValidator != null) {
				exceptionValidator.accept(e);
				return;
			}
			throw new AssertionError(e);
		}
	}
}
