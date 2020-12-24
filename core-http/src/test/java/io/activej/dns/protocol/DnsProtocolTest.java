package io.activej.dns.protocol;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.MalformedDataException;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.InetAddress;

import static io.activej.bytebuf.ByteBufPool.append;
import static io.activej.dns.protocol.DnsProtocol.ResponseErrorCode.NO_ERROR;
import static org.junit.Assert.*;

public class DnsProtocolTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final byte[] PREFIX = new byte[]{-127, -127, -127, -128, 0, 1, 0, 1, 0, 0, 0, 0, 3, 119, 119, 119, 4, 116, 101, 115, 116, 3, 99, 111, 109, 0, 0, 1, 0, 1};
	private static final byte[] SUFFIX = new byte[]{0, 1, 0, 1, 0, 0, 1, 13, 0, 4, -84, -39, 16, 36};

	@Test
	public void testMessageCompressedWithAPointer() throws MalformedDataException {
		ByteBuf pointer = ByteBuf.wrapForReading(new byte[]{(byte) 0b11000000, 12});
		doTestCompression(pointer);
	}

	@Test
	public void testMessageCompressedWithASequenceOfLabelsAndZeroOctet() throws MalformedDataException {
		ByteBuf labels = ByteBuf.wrapForReading(new byte[]{
				3, 'w', 'w', 'w',
				4, 't', 'e', 's', 't',
				3, 'c', 'o', 'm',
		});
		byte[] zeroOctet = {0};
		doTestCompression(append(labels, zeroOctet));
	}

	@Test
	public void testMessageCompressedWithASequenceOfLabelsAndAPointer() throws MalformedDataException {
		ByteBuf labels = ByteBuf.wrapForReading(new byte[]{
				3, 'w', 'w', 'w',
				4, 't', 'e', 's', 't',
				3, 'c', 'o', 'm'
		});
		byte[] pointer = {(byte) 0b11000000, 12};
		doTestCompression(append(labels, pointer));
	}

	private void doTestCompression(ByteBuf compressedPart) throws MalformedDataException {
		ByteBuf prefixBuf = ByteBuf.wrapForReading(PREFIX);
		ByteBuf suffixBuf = ByteBuf.wrapForReading(SUFFIX);
		ByteBuf payload = append(append(prefixBuf, compressedPart), suffixBuf);

		DnsResponse dnsResponse = DnsProtocol.readDnsResponse(payload);
		payload.recycle();

		assertSame(NO_ERROR, dnsResponse.getErrorCode());

		DnsResourceRecord record = dnsResponse.getRecord();
		assertNotNull(record);
		InetAddress[] ips = record.getIps();
		assertEquals(1, ips.length);
		assertEquals("www.test.com", ips[0].getHostName());
		assertArrayEquals(new byte[]{-84, -39, 16, 36}, ips[0].getAddress());
	}
}
