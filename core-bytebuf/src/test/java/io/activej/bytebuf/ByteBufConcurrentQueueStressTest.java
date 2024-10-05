package io.activej.bytebuf;

import io.activej.common.ref.RefInt;

import java.util.ArrayList;
import java.util.List;

public class ByteBufConcurrentQueueStressTest {
	private static final byte[] bytes = new byte[1];
	public static final int ROUNDS = 100;
	public static final int THREADS = 20;
	public static final int ITERATIONS = 100_000;

	public static void main(String[] args) throws InterruptedException {
		round();
	}

	private static void round() throws InterruptedException {
		long start = System.currentTimeMillis();
		RefInt allocations = new RefInt(0);
		ByteBufConcurrentQueue queue = new ByteBufConcurrentQueue();
		for (int round = 0; round < ROUNDS; round++) {
			List<Thread> threads = new ArrayList<>();
			for (int t = 0; t < THREADS; t++) {
				Thread thread = new Thread(() -> {
					for (int i = 0; i < ITERATIONS; i++) {
						ByteBuf buf = queue.poll();
						if (buf == null) {
							buf = ByteBuf.wrapForReading(bytes);
							allocations.inc();
						}
						queue.offer(buf);
					}
				});
				threads.add(thread);
			}
			for (Thread thread : threads) {
				thread.start();
			}
			for (Thread thread : threads) {
				thread.join();
			}
			System.out.println(queue);
			int operations = (round + 1) * THREADS * ITERATIONS;
			System.out.println("Operations: " + operations);
			System.out.println("Allocations: " + allocations.get());
			System.out.println("Size: " + queue.size());
			System.out.println("Ops/ms: " + operations / (System.currentTimeMillis() - start));
		}
	}
}
