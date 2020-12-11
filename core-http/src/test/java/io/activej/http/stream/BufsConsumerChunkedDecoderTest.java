package io.activej.http.stream;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.activej.http.TestUtils.AssertingConsumer;
import static io.activej.http.TestUtils.chunkedByByte;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

public final class BufsConsumerChunkedDecoderTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	public final AssertingConsumer consumer = new AssertingConsumer();
	public final List<ByteBuf> list = new ArrayList<>();
	public final BufsConsumerChunkedDecoder chunkedDecoder = BufsConsumerChunkedDecoder.create();

	@Before
	public void setUp() {
		list.clear();
		consumer.reset();
		chunkedDecoder.getOutput().set(consumer);
	}

	@Test
	public void shouldIgnoreChunkExtAfterNotLastChunk() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message = "2\r\nab\r\n5;name=value\r\nabcde\r\n0\r\n\r\n";
		decodeOneString(message, null);
	}

	@Test
	public void shouldIgnoreChunkExtAfterLastChunk() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message = "2\r\nab\r\n5\r\nabcde\r\n0;name=value\r\n\r\n";
		decodeOneString(message, null);
	}

	@Test
	public void shouldIgnoreChunkExtAfterChunkInAnotherBuf() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\nabcde\r\n0";
		String message2 = ";name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void shouldIgnoreChunkExtAfterChunkSemicolonInSameBuf() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\nabcde\r\n0;";
		String message2 = "name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void shouldWorkWithSizeCRLFInNextBuf() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5";
		String message2 = "\r\nabcde\r\n0;name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void shouldWorkWithSizeCRLFInSameBuf() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\n";
		String message2 = "abcde\r\n0;name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void shouldWorkWithCRLFInDifferentBufs() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5;abcd\r";
		String message2 = "\nabcde\r\n0;name=value\r\n\r\n";
		decodeTwoStrings(message1, message2);
	}

	@Test
	public void testForSplitChunkSize() {
		consumer.setExpectedByteArray("1234567890123456789".getBytes());
		String message1 = "1";
		String message2 = "3;asdasdasdasd\r\n";
		String message3 = "1234567890123456789\r\n0\r\n\r\n";
		decodeThreeStrings(message1, message2, message3);
	}

	@Test
	public void shouldThrowChunkSizeException() {
		consumer.setExpectedExceptionType(InvalidSizeException.class);
		String message = Integer.toHexString(-1) + "\r\n";
		decodeOneString(message, InvalidSizeException.class);
	}

	@Test
	public void shouldThrowParseException() {
		consumer.setExceptionValidator(e -> {
			assertThat(e, instanceOf(MalformedDataException.class));
			assertThat(e.getMessage(), startsWith("Array of bytes differs at index 0"));
		});
		String message = Integer.toHexString(1);
		message += "\r\nssss\r\n";
		decodeOneString(message, MalformedDataException.class);
	}

	@Test
	public void shouldIgnoreTrailerPart() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message = "2\r\nab\r\n5\r\nabcde\r\n0\r\ntrailer1\r\ntrailer2\r\n\r\n";
		decodeOneString(message, null);
	}

	@Test
	public void shouldIgnoreTrailerPartInMultipleBufs() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\nabcde\r\n0\r\ntra";
		String message2 = "iler1\r\ntra";
		String message3 = "iler2\r\n\r\n";
		decodeThreeStrings(message1, message2, message3);
	}

	@Test
	public void shouldIgnoreTrailerPartInDifferentBufs() {
		consumer.setExpectedByteArray("ababcde".getBytes());
		String message1 = "2\r\nab\r\n5\r\nabcde\r\n0\r\ntra";
		String message2 = "iler1\r\ntrailer2\r\n";
		String message3 = "trailer3\r\n\r\n";
		decodeThreeStrings(message1, message2, message3);
	}

	private void decodeOneString(String message, @Nullable Class<? extends Throwable> exceptionType) {
		byte[] bytes = message.getBytes();
		ByteBuf buf = ByteBufPool.allocate(bytes.length);
		buf.put(bytes);
		list.add(buf);

		doTest(exceptionType);
	}

	private void decodeTwoStrings(String message1, String message2) {
		byte[] bytes1 = message1.getBytes();
		byte[] bytes2 = message2.getBytes();
		ByteBuf buf1 = ByteBufPool.allocate(bytes1.length);
		ByteBuf buf2 = ByteBufPool.allocate(bytes2.length);
		buf1.put(bytes1);
		buf2.put(bytes2);
		list.add(buf1);
		list.add(buf2);

		doTest(null);
	}

	private void decodeThreeStrings(String message1, String message2, String message3) {
		byte[] bytes1 = message1.getBytes();
		byte[] bytes2 = message2.getBytes();
		byte[] bytes3 = message3.getBytes();
		ByteBuf buf1 = ByteBufPool.allocate(bytes1.length);
		ByteBuf buf2 = ByteBufPool.allocate(bytes2.length);
		ByteBuf buf3 = ByteBufPool.allocate(bytes3.length);
		buf1.put(bytes1);
		buf2.put(bytes2);
		buf3.put(bytes3);
		list.add(buf1);
		list.add(buf2);
		list.add(buf3);

		doTest(null);
	}

	private void doTest(@Nullable Class<? extends Throwable> expectedExceptionType) {
		chunkedDecoder.getInput().set(BinaryChannelSupplier.of(chunkedByByte(ChannelSupplier.ofList(list))));
		Promise<?> processResult = chunkedDecoder.getProcessCompletion();
		if (expectedExceptionType == null) {
			await(processResult);
		} else {
			Throwable actualException = awaitException(processResult);
			assertThat(actualException, instanceOf(expectedExceptionType));
		}
	}
}
