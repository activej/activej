package io.activej.crdt.storage.cluster;

import io.activej.common.exception.MalformedDataException;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;

public class PartitionIdTest {

	private static final String HOSTNAME = "255.255.255.255";

	@Test
	public void testLocalEquality() {
		String id = "test";
		PartitionId localPartitionId = PartitionId.of(id, null, null);
		PartitionId partitionId = PartitionId.of(id, address(9000), address(9001));

		assertEquals(localPartitionId, partitionId);
		assertEquals(localPartitionId.hashCode(), partitionId.hashCode());
	}

	@Test
	public void testParse() {
		doTestParse("test||", PartitionId.of("test", null, null));
		doTestParse("test|255.255.255.255:9000|", PartitionId.ofCrdtAddress("test", address(9000)));
		doTestParse("test||255.255.255.255:9000", PartitionId.ofRpcAddress("test", address(9000)));
		doTestParse("test|255.255.255.255:9000|255.255.255.255:9001", PartitionId.of("test", address(9000), address(9001)));
	}

	@Test
	public void testParseTolerant() {
		doTestParse(PartitionId.of("test", null, null), "test");
		doTestParse(PartitionId.ofCrdtAddress("test", address(9000)), "test|255.255.255.255:9000");
	}

	private void doTestParse(String expectedString, PartitionId rendezvousPartition) {
		String partitionIdString = rendezvousPartition.toString();
		assertEquals(expectedString, partitionIdString);

		PartitionId partitionId;
		try {
			partitionId = PartitionId.parseString(partitionIdString);
		} catch (MalformedDataException e) {
			throw new AssertionError(e);
		}

		assertEquals(rendezvousPartition.getId(), partitionId.getId());
		assertEquals(rendezvousPartition.getCrdtAddress(), partitionId.getCrdtAddress());
		assertEquals(rendezvousPartition.getRpcAddress(), partitionId.getRpcAddress());
	}

	private void doTestParse(PartitionId expectedPartitionId, String sourceString) {
		PartitionId partitionId;
		try {
			partitionId = PartitionId.parseString(sourceString);
		} catch (MalformedDataException e) {
			throw new AssertionError(e);
		}

		assertEquals(expectedPartitionId.getId(), partitionId.getId());
		assertEquals(expectedPartitionId.getCrdtAddress(), partitionId.getCrdtAddress());
		assertEquals(expectedPartitionId.getRpcAddress(), partitionId.getRpcAddress());
	}

	private static InetSocketAddress address(int port) {
		return new InetSocketAddress(HOSTNAME, port);
	}
}
