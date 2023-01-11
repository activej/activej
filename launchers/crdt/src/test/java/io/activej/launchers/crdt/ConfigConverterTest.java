package io.activej.launchers.crdt;

import io.activej.config.Config;
import io.activej.config.converter.ConfigConverter;
import io.activej.crdt.storage.cluster.PartitionId;
import io.activej.crdt.storage.cluster.PartitionScheme_Rendezvous;
import io.activej.crdt.storage.cluster.RendezvousPartitionGroup;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.*;

import static io.activej.launchers.crdt.ConfigConverters.*;
import static org.junit.Assert.*;

public class ConfigConverterTest {
	@Test
	public void ofSimplePartitionIdTest() {
		Map<String, String> properties = Map.of(
				"partition1", "testA",
				"partition2", "testB|255.255.255.255:9000",
				"partition3", "testC||255.255.255.255:9001",
				"partition4", "testD|127.0.0.1:9000|127.0.0.1:9001",
				"localPartition", "testD|localhost:9000|localhost:9001"
		);
		Config config = Config.ofMap(properties);
		ConfigConverter<PartitionId> converter = ofPartitionId();

		PartitionId partitionId1 = config.get(converter, "partition1");
		PartitionId expected1 = PartitionId.of("testA", null, null);
		assertPartitionsFullyEquals(expected1, partitionId1);

		PartitionId partitionId2 = config.get(converter, "partition2");
		PartitionId expected2 = PartitionId.of("testB",
				new InetSocketAddress("255.255.255.255", 9000),
				null
		);
		assertPartitionsFullyEquals(expected2, partitionId2);

		PartitionId partitionId3 = config.get(converter, "partition3");
		PartitionId expected3 = PartitionId.of("testC",
				null,
				new InetSocketAddress("255.255.255.255", 9001)
		);
		assertPartitionsFullyEquals(expected3, partitionId3);

		PartitionId partitionId4 = config.get(converter, "partition4");
		PartitionId expected4 = PartitionId.of("testD",
				new InetSocketAddress("127.0.0.1", 9000),
				new InetSocketAddress("127.0.0.1", 9001)
		);
		assertPartitionsFullyEquals(expected4, partitionId4);

		PartitionId localPartitionId = config.get(converter, "localPartition");
		PartitionId localExpected = PartitionId.of("testD",
				new InetSocketAddress("localhost", 9000),
				new InetSocketAddress("localhost", 9001)
		);
		assertPartitionsFullyEquals(localExpected, localPartitionId);
		assertEquals(partitionId4, localPartitionId);
	}

	@Test
	public void ofSimplePartitionGroupTest() {
		Map<String, String> properties = Map.of(
				"ids", "testA|101.101.101.101:9000|101.101.101.101:9001," +
						"testB|102.102.102.102:9000|102.102.102.102:9001," +
						"testC|103.103.103.103:9000|103.103.103.103:9001," +
						"testD|104.104.104.104:9000|104.104.104.104:9001",
				"replicas", "2",
				"repartition", "true",
				"active", "true"
		);
		Config config = Config.ofMap(properties);
		ConfigConverter<RendezvousPartitionGroup<PartitionId>> converter = ofPartitionGroup(ofPartitionId());

		RendezvousPartitionGroup<PartitionId> partitionGroup = converter.get(config);

		Set<PartitionId> expectedPartitionIds = Set.of(
				PartitionId.of("testA",
						new InetSocketAddress("101.101.101.101", 9000),
						new InetSocketAddress("101.101.101.101", 9001)
				),
				PartitionId.of("testB",
						new InetSocketAddress("102.102.102.102", 9000),
						new InetSocketAddress("102.102.102.102", 9001)
				),
				PartitionId.of("testC",
						new InetSocketAddress("103.103.103.103", 9000),
						new InetSocketAddress("103.103.103.103", 9001)
				),
				PartitionId.of("testD",
						new InetSocketAddress("104.104.104.104", 9000),
						new InetSocketAddress("104.104.104.104", 9001)
				)
		);

		assertSetsFullyEquals(expectedPartitionIds, partitionGroup.getPartitionIds());
		assertEquals(2, partitionGroup.getReplicaCount());
		assertTrue(partitionGroup.isActive());
		assertTrue(partitionGroup.isRepartition());
	}

	@Test
	public void ofSimplePartitionSchemeTest() {
		Map<String, String> properties = new HashMap<>();

		properties.put("partitionGroup.1.ids", "" +
				"testA|101.101.101.101:9000|101.101.101.101:9001," +
				"testB|102.102.102.102:9000|102.102.102.102:9001," +
				"testC|103.103.103.103:9000|103.103.103.103:9001," +
				"testD|104.104.104.104:9000|104.104.104.104:9001");
		properties.put("partitionGroup.1.replicas", "3");
		properties.put("partitionGroup.1.repartition", "true");
		properties.put("partitionGroup.1.active", "true");

		properties.put("partitionGroup.2.ids", "" +
				"testE|105.105.105.105:9000|105.105.105.105:9001," +
				"testF|106.106.106.106:9000|106.106.106.106:9001," +
				"testG|107.107.107.107:9000|107.107.107.107:9001");
		properties.put("partitionGroup.2.replicas", "2");
		properties.put("partitionGroup.2.repartition", "false");
		properties.put("partitionGroup.2.active", "false");

		Config config = Config.ofMap(properties);
		ConfigConverter<PartitionScheme_Rendezvous<PartitionId>> converter = ofRendezvousPartitionScheme(ofPartitionId());

		PartitionScheme_Rendezvous<PartitionId> partitionScheme = converter.get(config);
		Set<PartitionId> partitions = partitionScheme.getPartitions();

		Set<PartitionId> expectedPartitionIds = Set.of(
				PartitionId.of("testA",
						new InetSocketAddress("101.101.101.101", 9000),
						new InetSocketAddress("101.101.101.101", 9001)
				),
				PartitionId.of("testB",
						new InetSocketAddress("102.102.102.102", 9000),
						new InetSocketAddress("102.102.102.102", 9001)
				),
				PartitionId.of("testC",
						new InetSocketAddress("103.103.103.103", 9000),
						new InetSocketAddress("103.103.103.103", 9001)
				),
				PartitionId.of("testD",
						new InetSocketAddress("104.104.104.104", 9000),
						new InetSocketAddress("104.104.104.104", 9001)
				),
				PartitionId.of("testE",
						new InetSocketAddress("105.105.105.105", 9000),
						new InetSocketAddress("105.105.105.105", 9001)
				),
				PartitionId.of("testF",
						new InetSocketAddress("106.106.106.106", 9000),
						new InetSocketAddress("106.106.106.106", 9001)
				),
				PartitionId.of("testG",
						new InetSocketAddress("107.107.107.107", 9000),
						new InetSocketAddress("107.107.107.107", 9001)
				)
		);

		assertSetsFullyEquals(expectedPartitionIds, partitions);
	}

	private static void assertPartitionsFullyEquals(PartitionId expected, PartitionId actual) {
		assertEquals(expected.getId(), actual.getId());
		assertEquals(expected.getCrdtAddress(), actual.getCrdtAddress());
		assertEquals(expected.getRpcAddress(), actual.getRpcAddress());
	}

	private static void assertSetsFullyEquals(Set<PartitionId> expected, Set<PartitionId> actual) {
		assertEquals(expected.size(), actual.size());

		expected = sort(expected);
		actual = sort(actual);

		Iterator<PartitionId> expectedIt = expected.iterator();
		Iterator<PartitionId> actualIt = actual.iterator();

		while (expectedIt.hasNext()) {
			assertPartitionsFullyEquals(expectedIt.next(), actualIt.next());
		}

		assertFalse(actualIt.hasNext());
	}

	private static SortedSet<PartitionId> sort(Set<PartitionId> set) {
		TreeSet<PartitionId> treeSet = new TreeSet<>(Comparator.comparing(PartitionId::getId));
		treeSet.addAll(set);
		return treeSet;
	}
}
