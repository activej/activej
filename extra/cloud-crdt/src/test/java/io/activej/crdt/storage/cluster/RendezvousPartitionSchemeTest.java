package io.activej.crdt.storage.cluster;

import io.activej.crdt.storage.cluster.IDiscoveryService.PartitionScheme;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RendezvousPartitionSchemeTest {

	@Test
	public void testSameIds() {
		PartitionScheme<PartitionId> partitionings = RendezvousPartitionScheme.<PartitionId>builder()
				.withPartitionGroup(RendezvousPartitionGroup.builder(
								Set.of(
										PartitionId.of("a", new InetSocketAddress(9001), new InetSocketAddress(8001)),
										PartitionId.of("b", new InetSocketAddress(9002), new InetSocketAddress(8002)),
										PartitionId.of("c", new InetSocketAddress(9003), new InetSocketAddress(8003))
								)
						)
						.withReplicas(1)
						.withRepartition(false)
						.withActive(true)
						.build())
				.withPartitionGroup(RendezvousPartitionGroup.builder(
								Set.of(
										PartitionId.of("a", new InetSocketAddress(9004), new InetSocketAddress(8004)),
										PartitionId.of("b", new InetSocketAddress(9005), new InetSocketAddress(8005)),
										PartitionId.of("c", new InetSocketAddress(9006), new InetSocketAddress(8006))
								)
						)
						.withReplicas(1)
						.withRepartition(false)
						.withActive(false)
						.build())
				.withPartitionIdGetter(PartitionId::getId)
				.build();

		List<PartitionId> alive = List.of(
				PartitionId.of("a", new InetSocketAddress(9001), new InetSocketAddress(8001)),
				PartitionId.of("b", new InetSocketAddress(9002), new InetSocketAddress(8002)),
				PartitionId.of("c", new InetSocketAddress(9003), new InetSocketAddress(8003)),

				PartitionId.of("a", new InetSocketAddress(9004), new InetSocketAddress(8004)),
				PartitionId.of("b", new InetSocketAddress(9005), new InetSocketAddress(8005)),
				PartitionId.of("c", new InetSocketAddress(9006), new InetSocketAddress(8006))
		);

		Sharder<Integer> sharder = partitionings.createSharder(alive);

		assert sharder != null;

		for (int i = 0; i < 1_000_000; i++) {
			int[] sharded = sharder.shard(i);

			assertEquals(2, sharded.length);
			assertEquals(sharded[0], sharded[1] - 3);
		}
	}
}
