package io.activej.bytebuf;

import java.util.ArrayList;
import java.util.List;

public class ByteBufConcurrentQueueStressTest {
	private static final byte[] bytes = new byte[1];

	public static void main(String[] args) throws InterruptedException {
		round();
	}

	private static void round() throws InterruptedException {
		ByteBufConcurrentQueue queue = new ByteBufConcurrentQueue();
		for (int x = 0; x < 100; x++) {
			List<Thread> threads = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				int finalI = i;
				Thread thread = new Thread(() -> {
					for (int j = 0; j < 10_000; j++) {
						ByteBuf buf = queue.poll();
						if (buf == null) {
							buf = ByteBuf.wrapForReading(bytes);
						}
						buf.pos = finalI;
						if (buf.pos != finalI) throw new AssertionError();
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
			int count = (x + 1) * 20 * 10_000;
			System.out.println("Polled: " + count);
			System.out.println("Offered: " + count);
			System.out.println("Offer long path: " + queue.offerLongPath);
			System.out.println("Poll long path: " + queue.pollLongPath);
			System.out.println("Poll long path ops: " + queue.pollLongPathOps);
			System.out.println("Thread yielded: " + queue.threadYielded);

		}
	}
}
