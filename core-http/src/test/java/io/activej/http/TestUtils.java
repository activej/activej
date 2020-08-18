package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.MemSize;
import io.activej.csp.AbstractChannelConsumer;
import io.activej.csp.process.ChannelByteChunker;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
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

	private static final Random RANDOM = ThreadLocalRandom.current();

	public static final PingPongHandler HANDLER_STUB = PingPongHandler.of(ByteBuf::recycle, ByteBuf::recycle);

	public static ChannelByteChunker chunker() {
		MemSize min = MemSize.of(RANDOM.nextInt(5) + 1);
		return ChannelByteChunker.create(min, min.map(length -> length * 2));
	}

	public static byte[] randomBytes(int size){
		byte[] bytes = new byte[size];
		RANDOM.nextBytes(bytes);
		return bytes;
	}

	public static ByteBuf closeUnmasked() {
		return ByteBuf.wrapForReading(new byte[]{(byte) 0x88, 0x02, 0x03, (byte) 0xe8});
	}

	public static ByteBuf closeMasked() {
		return ByteBuf.wrapForReading(new byte[]{(byte) 0x88, (byte) 0x82, 0x12, 0x34, 0x56, 0x78, 0x11, (byte) 0xdc});
	}
}
