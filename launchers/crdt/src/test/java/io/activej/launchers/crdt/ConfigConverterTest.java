package io.activej.launchers.crdt;

import io.activej.config.Config;
import io.activej.config.converter.ConfigConverter;
import io.activej.crdt.storage.cluster.RendezvousPartitioning;
import io.activej.crdt.storage.cluster.RendezvousPartitionings;
import io.activej.crdt.storage.cluster.SimplePartitionId;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.*;

import static io.activej.common.Utils.mapOf;
import static io.activej.common.Utils.setOf;
import static io.activej.launchers.crdt.ConfigConverters.*;
import static org.junit.Assert.*;

public class ConfigConverterTest {
	@Test
	public void ofSimplePartitionIdTest() {
		Map<String, String> properties = mapOf(
				"partition1", "testA",
				"partition2", "testB|255.255.255.255:9000",
				"partition3", "testC||255.255.255.255:9001",
				"partition4", "testD|255.255.255.255:9000|255.255.255.255:9001",
				"localPartition", "testD|localhost:9000|localhost:9001"
		);
		Config config = Config.ofMap(properties);
		ConfigConverter<SimplePartitionId> converter = ofSimplePartitionId();

		SimplePartitionId partitionId1 = config.get(converter, "partition1");
		SimplePartitionId expected1 = SimplePartitionId.of("testA", null, null);
		assertPartitionsFullyEquals(expected1, partitionId1);

		SimplePartitionId partitionId2 = config.get(converter, "partition2");
		SimplePartitionId expected2 = SimplePartitionId.of("testB",
				new InetSocketAddress("255.255.255.255", 9000),
				null
		);
		assertPartitionsFullyEquals(expected2, partitionId2);

		SimplePartitionId partitionId3 = config.get(converter, "partition3");
		SimplePartitionId expected3 = SimplePartitionId.of("testC",
				null,
				new InetSocketAddress("255.255.255.255", 9001)
		);
		assertPartitionsFullyEquals(expected3, partitionId3);

		SimplePartitionId partitionId4 = config.get(converter, "partition4");
		SimplePartitionId expected4 = SimplePartitionId.of("testD",
				new InetSocketAddress("255.255.255.255", 9000),
				new InetSocketAddress("255.255.255.255", 9001)
		);
		assertPartitionsFullyEquals(expected4, partitionId4);

		SimplePartitionId localPartitionId = config.get(converter, "localPartition");
		SimplePartitionId localExpected = SimplePartitionId.of("testD",
				new InetSocketAddress("localhost", 9000),
				new InetSocketAddress("localhost", 9001)
		);
		assertPartitionsFullyEquals(localExpected, localPartitionId);
		assertEquals(partitionId4, localPartitionId);
	}

	@Test
	public void ofSimplePartitioningTest() {
		Map<String, String> properties = mapOf(
				"ids", "testA|101.101.101.101:9000|101.101.101.101:9001," +
						"testB|102.102.102.102:9000|102.102.102.102:9001," +
						"testC|103.103.103.103:9000|103.103.103.103:9001," +
						"testD|104.104.104.104:9000|104.104.104.104:9001",
				"replicas", "2",
				"repartition", "true",
				"active", "true"
		);
		Config config = Config.ofMap(properties);
		ConfigConverter<RendezvousPartitioning<SimplePartitionId>> converter = ofPartitioning(ofSimplePartitionId());

		RendezvousPartitioning<SimplePartitionId> partitioning = converter.get(config);

		Set<SimplePartitionId> expectedPartitionIds = setOf(
				SimplePartitionId.of("testA",
						new InetSocketAddress("101.101.101.101", 9000),
						new InetSocketAddress("101.101.101.101", 9001)
				),
				SimplePartitionId.of("testB",
						new InetSocketAddress("102.102.102.102", 9000),
						new InetSocketAddress("102.102.102.102", 9001)
				),
				SimplePartitionId.of("testC",
						new InetSocketAddress("103.103.103.103", 9000),
						new InetSocketAddress("103.103.103.103", 9001)
				),
				SimplePartitionId.of("testD",
						new InetSocketAddress("104.104.104.104", 9000),
						new InetSocketAddress("104.104.104.104", 9001)
				)
		);

		assertSetsFullyEquals(expectedPartitionIds, partitioning.getPartitions());
		assertEquals(2, partitioning.getReplicas());
		assertTrue(partitioning.isActive());
		assertTrue(partitioning.isRepartition());
	}

	@Test
	public void ofSimplePartitioningsTest() {
		Map<String, String> properties = new HashMap<>();

		properties.put("partitionings.1.ids", "" +
				"testA|101.101.101.101:9000|101.101.101.101:9001," +
				"testB|102.102.102.102:9000|102.102.102.102:9001," +
				"testC|103.103.103.103:9000|103.103.103.103:9001," +
				"testD|104.104.104.104:9000|104.104.104.104:9001");
		properties.put("partitionings.1.replicas", "3");
		properties.put("partitionings.1.repartition", "true");
		properties.put("partitionings.1.active", "true");

		properties.put("partitionings.2.ids", "" +
				"testE|105.105.105.105:9000|105.105.105.105:9001," +
				"testF|106.106.106.106:9000|106.106.106.106:9001," +
				"testG|107.107.107.107:9000|107.107.107.107:9001");
		properties.put("partitionings.2.replicas", "2");
		properties.put("partitionings.2.repartition", "false");
		properties.put("partitionings.2.active", "false");

		Config config = Config.ofMap(properties);
		ConfigConverter<RendezvousPartitionings<SimplePartitionId>> converter = ofRendezvousPartitionings();

		RendezvousPartitionings<SimplePartitionId> partitionings = converter.get(config);
		Set<SimplePartitionId> partitions = partitionings.getPartitions();

		Set<SimplePartitionId> expectedPartitionIds = setOf(
				SimplePartitionId.of("testA",
						new InetSocketAddress("101.101.101.101", 9000),
						new InetSocketAddress("101.101.101.101", 9001)
				),
				SimplePartitionId.of("testB",
						new InetSocketAddress("102.102.102.102", 9000),
						new InetSocketAddress("102.102.102.102", 9001)
				),
				SimplePartitionId.of("testC",
						new InetSocketAddress("103.103.103.103", 9000),
						new InetSocketAddress("103.103.103.103", 9001)
				),
				SimplePartitionId.of("testD",
						new InetSocketAddress("104.104.104.104", 9000),
						new InetSocketAddress("104.104.104.104", 9001)
				),
				SimplePartitionId.of("testE",
						new InetSocketAddress("105.105.105.105", 9000),
						new InetSocketAddress("105.105.105.105", 9001)
				),
				SimplePartitionId.of("testF",
						new InetSocketAddress("106.106.106.106", 9000),
						new InetSocketAddress("106.106.106.106", 9001)
				),
				SimplePartitionId.of("testG",
						new InetSocketAddress("107.107.107.107", 9000),
						new InetSocketAddress("107.107.107.107", 9001)
				)
		);

		assertSetsFullyEquals(expectedPartitionIds, partitions);
	}

	private static void assertPartitionsFullyEquals(SimplePartitionId expected, SimplePartitionId actual) {
		assertEquals(expected.getId(), actual.getId());
		assertEquals(expected.getCrdtAddress(), actual.getCrdtAddress());
		assertEquals(expected.getRpcAddress(), actual.getRpcAddress());
	}

	private static void assertSetsFullyEquals(Set<SimplePartitionId> expected, Set<SimplePartitionId> actual) {
		assertEquals(expected.size(), actual.size());

		expected = sort(expected);
		actual = sort(actual);

		Iterator<SimplePartitionId> expectedIt = expected.iterator();
		Iterator<SimplePartitionId> actualIt = actual.iterator();

		while (expectedIt.hasNext()){
			assertPartitionsFullyEquals(expectedIt.next(), actualIt.next());
		}

		assertFalse(actualIt.hasNext());
	}

	private static SortedSet<SimplePartitionId> sort(Set<SimplePartitionId> set) {
		TreeSet<SimplePartitionId> treeSet = new TreeSet<>(Comparator.comparing(SimplePartitionId::getId));
		treeSet.addAll(set);
		return treeSet;
	}
}
