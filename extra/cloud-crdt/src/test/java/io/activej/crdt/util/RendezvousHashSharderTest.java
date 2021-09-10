package io.activej.crdt.util;

import io.activej.common.ref.RefInt;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.activej.common.Utils.keysToMap;
import static io.activej.common.Utils.setOf;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RendezvousHashSharderTest {

	@Test
	public void testSharder() {
		RefInt index = new RefInt(0);
		Map<String, Integer> partitionsWithIndexes = keysToMap(setOf("one", "two", "three", "four", "five")
				.stream()
				.sorted(), s -> index.value++);
		Set<String> partitions = new HashSet<>(partitionsWithIndexes.keySet());
		RendezvousHashSharder<String> sharder = RendezvousHashSharder.create(partitions, 3);

		Map<Integer, Set<Integer>> sharded = new HashMap<>();
		for (int i = 0; i < 10; i++) {
			for (int id : sharder.shard(i)) {
				sharded.computeIfAbsent(i, $ -> new HashSet<>()).add(id);
			}
		}
		Set<Integer> allSharded = sharded.values().stream()
				.flatMap(Collection::stream)
				.collect(toSet());

		assertEquals(new HashSet<>(partitionsWithIndexes.values()), allSharded);

		partitions.remove("five");
		int fiveId = partitionsWithIndexes.remove("five");
		sharder.recompute(partitions);

		Map<Integer, Set<Integer>> sharded2 = new HashMap<>();
		for (int i = 0; i < 10; i++) {
			for (int id : sharder.shard(i)) {
				sharded2.computeIfAbsent(i, $ -> new HashSet<>()).add(id);
			}
		}
		for (Map.Entry<Integer, Set<Integer>> entry : sharded.entrySet()) {
			Set<Integer> first = entry.getValue();
			Set<Integer> second = sharded2.get(entry.getKey());
			for (int id : first) {
				if (id != fiveId) {
					assertTrue(second.contains(id));
				}
			}
		}

		allSharded = sharded2.values().stream()
				.flatMap(Collection::stream)
				.collect(toSet());

		assertEquals(new HashSet<>(partitionsWithIndexes.values()), allSharded);

		partitions.remove("four");
		sharder.recompute(partitions);

		List<int[]> shards = IntStream.range(0, 100)
				.mapToObj(sharder::shard)
				.collect(Collectors.toList());
		int[] first = shards.get(0);
		assertTrue(shards.stream().allMatch(ints -> Arrays.equals(first, ints)));
	}
}
