import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;

import java.util.Arrays;

public final class ByteBufsExample {
	private static final ByteBufs BUFS = new ByteBufs();

	private static void addingToBufs() {
		//[START REGION_1]
		BUFS.add(ByteBuf.wrapForReading(new byte[]{0, 1, 2, 3}));
		BUFS.add(ByteBuf.wrapForReading(new byte[]{3, 4, 5}));

		// bufs consist of 2 Bufs at this moment
		//[END REGION_1]
		System.out.println(BUFS);
		System.out.println();
	}

	private static void takingBufOutOfBufs() {
		ByteBuf takenBuf = BUFS.take();

		// Buf that is taken is the one that was put in bufs first
		System.out.println("Buf taken from bufs: " + Arrays.toString(takenBuf.asArray()));
		System.out.println();
	}

	private static void takingEverythingOutOfBufs() {
		//[START REGION_2]
		// Adding one more ByteBuf to bufs
		BUFS.add(ByteBuf.wrapForReading(new byte[]{6, 7, 8}));

		ByteBuf takenBuf = BUFS.takeRemaining();

		// Taken ByteBuf is combined of every ByteBuf that were in bufs
		//[END REGION_2]
		System.out.println("Buf taken from bufs: " + Arrays.toString(takenBuf.asArray()));
		System.out.println("Is 'ByteBufs' empty? " + BUFS.isEmpty());
		System.out.println();
	}

	public static void main(String[] args) {
		addingToBufs();
		takingBufOutOfBufs();
		takingEverythingOutOfBufs();
	}
}
