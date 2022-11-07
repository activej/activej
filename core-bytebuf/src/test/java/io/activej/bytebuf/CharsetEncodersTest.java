package io.activej.bytebuf;

import io.activej.common.exception.MalformedDataException;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.activej.bytebuf.ByteBufTest.initByteBufPool;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public class CharsetEncodersTest {
	private static final String SMILES1 = "Hello 😵 World!";
	private static final String SMILES2 = "Hello \uD83D\uDE35 World!";

	static {
		initByteBufPool();
	}

	@Test
	public void testEncode() throws MalformedDataException {
		assertEquals(SMILES2, SMILES1);
		assertEncode(SMILES2, SMILES1);
		assertEncode("Миру - мир!", "Миру - мир!");
		assertReplacement("\uD83D ", " ");
		assertReplacement("BEGIN \uDE35 END", "BEGIN  END");
		assertReplacement("BEGIN \uD83D", "BEGIN ");
		assertReplacement("BEGIN \uDE35", "BEGIN ");
	}

	@Test
	public void testCharAfterInvalidSurrogate() throws MalformedDataException {
		String s = "==\uD83C\uDF46🍆🍆\uD83C\uDF46==";
		String s1 = s.substring(0, 5) + '\t';
		ByteBuf byteBuf = ByteBufStrings.wrapUtf8(s1);
		String s2 = ByteBufStrings.decodeUtf8(byteBuf);

		assertTrue(s2.endsWith("\t"));
	}

	private static void assertEncode(String s, String expected) throws MalformedDataException {
		byte[] encodedJava = encodeJava(s);
		String decodedJava = decodeJava(encodedJava);
		assertEquals(expected, decodedJava);

		byte[] encoded = encode(s);
		assertArrayEquals(encodedJava, encoded);

		String decoded = decode(encoded);
		assertEquals(expected, decoded);

		decoded = decode(encodedJava);
		assertEquals(expected, decoded);

		decodedJava = decodeJava(encoded);
		assertEquals(expected, decodedJava);
	}

	private static void assertReplacement(String s, String expected) throws MalformedDataException {
		byte[] encoded = encode(s);
		String decoded = decode(encoded);
		assertEquals(expected, decoded);
	}

	private static byte[] encodeJava(String s) {
		ByteBuffer buf = UTF_8.encode(s);
		return Arrays.copyOf(buf.array(), buf.limit());
	}

	private static String decodeJava(byte[] bytes) {
		return UTF_8.decode(ByteBuffer.wrap(bytes)).toString();
	}

	private static byte[] encode(String s) {
		byte[] bytes = new byte[s.length() * 3];
		int pos = ByteBufStrings.encodeUtf8(bytes, 0, s);
		return Arrays.copyOf(bytes, pos);
	}

	private static String decode(byte[] encoded) throws MalformedDataException {
		return ByteBufStrings.decodeUtf8(encoded);
	}
}
