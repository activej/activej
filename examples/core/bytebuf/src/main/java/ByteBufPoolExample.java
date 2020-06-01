import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;

import java.util.Arrays;

//[START EXAMPLE]
public final class ByteBufPoolExample {
	/* Setting ByteBufPool minSize and maxSize properties here for illustrative purposes.
	 Otherwise, ByteBufs with size less than 32 would not be placed into pool
	 */
	static {
		System.setProperty("ByteBufPool.minSize", "1");
	}

	private static void allocatingBufs() {
		// Allocating a ByteBuf of 100 bytes
		ByteBuf byteBuf = ByteBufPool.allocate(100);

		// Allocated ByteBuf has an array with size equal to next power of 2, hence 128
		System.out.println("Length of array of allocated ByteBuf: " + byteBuf.writeRemaining());

		// Pool has 0 ByteBufs right now
		System.out.println("Number of ByteBufs in pool before recycling: " + ByteBufPool.getStats().getPoolItems());

		// Recycling ByteBuf to put it back to pool
		byteBuf.recycle();

		// Now pool consists of 1 ByteBuf that is the one we just recycled
		System.out.println("Number of ByteBufs in pool after recycling: " + ByteBufPool.getStats().getPoolItems());

		// Trying to allocate another ByteBuf
		ByteBuf anotherByteBuf = ByteBufPool.allocate(123);

		// Pool is now empty as the only ByteBuf in pool has just been taken from the pool
		System.out.println("Number of ByteBufs in pool: " + ByteBufPool.getStats().getPoolItems());
		System.out.println();
	}

	private static void ensuringWriteRemaining() {
		ByteBuf byteBuf = ByteBufPool.allocate(3);

		// Size is equal to power of 2 that is larger than 3, hence 4
		System.out.println("Size of ByteBuf: " + byteBuf.writeRemaining());

		byteBuf.write(new byte[]{0, 1, 2});

		// After writing 3 bytes into ByteBuf we have only 1 spare byte in ByteBuf
		System.out.println("Remaining bytes of ByteBuf after 3 bytes have been written: " + byteBuf.writeRemaining());

		// We need to write 3 more bytes so we have to ensure that there are 3 spare bytes in ByteBuf
		// and if there are not - create new ByteBuf with enough room for 3 bytes (old ByteBuf will get recycled)
		ByteBuf newByteBuf = ByteBufPool.ensureWriteRemaining(byteBuf, 3);
		System.out.println("Amount of ByteBufs in pool:" + ByteBufPool.getStats().getPoolItems());

		// As we need to write 3 more bytes, we need a ByteBuf that can hold 6 bytes.
		// The next power of 2 is 8, so considering 3 bytes that have already been written, new ByteBuf
		// can store (8-3=5) more bytes
		System.out.println("Remaining bytes of a new ByteBuf: " + newByteBuf.writeRemaining());

		// Recycling a new ByteBuf (remember, the old one has already been recycled)
		newByteBuf.recycle();
		System.out.println();
	}

	private static void appendingBufs() {
		ByteBuf bufOne = ByteBuf.wrapForReading(new byte[]{0, 1, 2});
		ByteBuf bufTwo = ByteBuf.wrapForReading(new byte[]{3, 4, 5});

		ByteBuf appendedBuf = ByteBufPool.append(bufOne, bufTwo);

		// Appended ByteBuf consists of two ByteBufs, you don't have to worry about allocating ByteBuf
		// with enough capacity or how to properly copy bytes, ByteBufPool will handle it for you
		System.out.println(Arrays.toString(appendedBuf.asArray()));
		System.out.println();
	}

	public static void main(String[] args) {
		allocatingBufs();
		ensuringWriteRemaining();
		appendingBufs();
	}
}
//[END EXAMPLE]
