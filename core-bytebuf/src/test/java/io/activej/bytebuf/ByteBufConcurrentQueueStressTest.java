package io.activej.bytebuf;

import java.util.ArrayList;
import java.util.List;

public class ByteBufConcurrentQueueStressTest {
	private static final byte[] bytes = new byte[1];

	public static void main(String[] args) throws InterruptedException {
		for (int i = 0; i < 10000; i++) {
			round();
		}
	}

	private static void round() throws InterruptedException {
		ByteBufConcurrentQueue queue = new ByteBufConcurrentQueue();
		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 20; i++) {
			int finalI = i;
			Thread thread = new Thread(() -> {
				for (int j = 0; j < 10000; j++) {
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
	}
}
