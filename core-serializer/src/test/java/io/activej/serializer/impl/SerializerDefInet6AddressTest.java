package io.activej.serializer.impl;

import org.junit.Test;

import java.net.Inet6Address;
import java.net.UnknownHostException;

import static io.activej.serializer.Utils.doTest;
import static org.junit.Assert.assertEquals;

public final class SerializerDefInet6AddressTest {

	@Test
	public void testIPV4MappedAddresses() throws UnknownHostException {
		byte[] ipv4mappedAddr = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xFF, (byte) 0xFF, 0x7F, 0, 0, 1};
		Inet6Address address1 = Inet6Address.getByAddress(null, ipv4mappedAddr, null);

		Inet6Address address2 = doTest(Inet6Address.class, address1);
		assertEquals(address1, address2);
	}
}
