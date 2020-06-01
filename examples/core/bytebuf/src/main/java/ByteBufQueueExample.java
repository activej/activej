import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;

import java.util.Arrays;

public final class ByteBufQueueExample {
	private static final ByteBufQueue QUEUE = new ByteBufQueue();

	private static void addingBufsToQueue() {
		//[START REGION_1]
		QUEUE.add(ByteBuf.wrapForReading(new byte[]{0, 1, 2, 3}));
		QUEUE.add(ByteBuf.wrapForReading(new byte[]{3, 4, 5}));

		// queue consists of 2 Bufs at this moment
		//[END REGION_1]
		System.out.println(QUEUE);
		System.out.println();
	}

	private static void takingBufOutOfQueue() {
		ByteBuf takenBuf = QUEUE.take();

		// Buf that is taken is the one that was put in queue first
		System.out.println("Buf taken from queue: " + Arrays.toString(takenBuf.asArray()));
		System.out.println();
	}

	private static void takingEverythingOutOfQueue() {
		//[START REGION_2]
		// Adding one more ByteBuf to queue
		QUEUE.add(ByteBuf.wrapForReading(new byte[]{6, 7, 8}));

		ByteBuf takenBuf = QUEUE.takeRemaining();

		// Taken ByteBuf is combined of every ByteBuf that were in Queue
		//[END REGION_2]
		System.out.println("Buf taken from queue: " + Arrays.toString(takenBuf.asArray()));
		System.out.println("Is queue empty? " + QUEUE.isEmpty());
		System.out.println();
	}

	private static void drainingQueue() {
		//[START REGION_3]
		QUEUE.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 4}));
		QUEUE.add(ByteBuf.wrapForReading(new byte[]{5, 6, 7, 8}));

		// Draining queue to some ByteBuf consumer
		QUEUE.drainTo(buf -> System.out.println(Arrays.toString(buf.getArray())));

		// Queue is empty after draining
		//[END REGION_3]
		System.out.println("Is queue empty? " + QUEUE.isEmpty());
		System.out.println();
	}

	public static void main(String[] args) {
		addingBufsToQueue();
		takingBufOutOfQueue();
		takingEverythingOutOfQueue();
		drainingQueue();
	}
}
