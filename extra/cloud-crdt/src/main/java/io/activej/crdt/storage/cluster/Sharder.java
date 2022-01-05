package io.activej.crdt.storage.cluster;

import java.util.Arrays;
import java.util.List;
import java.util.function.ToIntFunction;

public interface Sharder<K> {
	int[] shard(K key);

	static <K> Sharder<K> none() {
		return key -> new int[0];
	}

	static <K> Sharder<K> unionOf(List<? extends Sharder<K>> sharders) {
		if (sharders.isEmpty()) return none();
		if (sharders.size() == 1) return sharders.get(0);
		boolean isRendezvous = true;
		RendezvousHashSharder<K, ?> prev = null;
		for (Sharder<K> sharder : sharders) {
			if (!(sharder instanceof RendezvousHashSharder)) {
				isRendezvous = false;
				break;
			}
			RendezvousHashSharder<K, ?> rendezvousSharder = (RendezvousHashSharder<K, ?>) sharder;
			if (prev != null && (rendezvousSharder.keyHashFn != prev.keyHashFn || rendezvousSharder.buckets.length != prev.buckets.length)) {
				isRendezvous = false;
				break;
			}
			prev = rendezvousSharder;
		}
		if (isRendezvous) {
			int[][] buckets = new int[((RendezvousHashSharder<K, Object>) sharders.get(0)).buckets.length][];
			ToIntFunction<K> keyHashFn = ((RendezvousHashSharder<K, Object>) sharders.get(0)).keyHashFn;
			int[] buf = new int[0];
			for (int bucket = 0; bucket < buckets.length; bucket++) {
				int pos = 0;
				for (Sharder<K> sharder : sharders) {
					RendezvousHashSharder<K, Object> rendezvousSharder = (RendezvousHashSharder<K, Object>) sharder;
					int[] selected = rendezvousSharder.buckets[bucket];
					NEXT:
					for (int idx : selected) {
						for (int i = 0; i < pos; i++) {
							if (idx == buf[i]) continue NEXT;
						}
						if (buf.length <= pos) buf = Arrays.copyOf(buf, buf.length * 2 + 1);
						buf[pos++] = idx;
					}
				}
				buckets[bucket] = Arrays.copyOf(buf, pos);
			}
			return new RendezvousHashSharder<>(buckets, keyHashFn);
		} else {
			return new Sharder<K>() {
				volatile int maxLength;

				@Override
				public int[] shard(K key) {
					int[] buf = new int[maxLength];
					int pos = 0;
					for (Sharder<K> sharder : sharders) {
						int[] selected = sharder.shard(key);
						NEXT:
						for (int idx : selected) {
							for (int i = 0; i < pos; i++) {
								if (idx == buf[i]) continue NEXT;
							}
							if (buf.length <= pos) {
								buf = Arrays.copyOf(buf, buf.length + 1);
								maxLength = buf.length;
							}
							buf[pos++] = idx;
						}
					}
					return Arrays.copyOf(buf, pos);
				}
			};
		}
	}

}
