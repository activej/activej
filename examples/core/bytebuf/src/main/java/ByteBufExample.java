import io.activej.bytebuf.ByteBuf;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ByteBufExample {
	private static void wrapForReading() {
		//[START REGION_1]
		byte[] data = new byte[]{0, 1, 2, 3, 4, 5};
		ByteBuf byteBuf = ByteBuf.wrapForReading(data);
		//[END REGION_1]
		while (byteBuf.canRead()) {
			System.out.println(byteBuf.readByte());
		}

		System.out.println();
	}

	private static void wrapForWriting() {
		//[START REGION_2]
		byte[] data = new byte[6];
		ByteBuf byteBuf = ByteBuf.wrapForWriting(data);
		byte value = 0;
		while (byteBuf.canWrite()) {
			byteBuf.writeByte(value++);
		}
		//[END REGION_2]
		System.out.println(Arrays.toString(byteBuf.getArray()));
		System.out.println();
	}

	private static void stringConversion() {
		//[START REGION_3]
		String message = "Hello";
		ByteBuf byteBuf = ByteBuf.wrapForReading(message.getBytes(UTF_8));
		String unWrappedMessage = byteBuf.asString(UTF_8);
		//[END REGION_3]
		System.out.println(unWrappedMessage);
		System.out.println();
	}

	private static void slicing() {
		//[START REGION_4]
		byte[] data = new byte[]{0, 1, 2, 3, 4, 5};
		ByteBuf byteBuf = ByteBuf.wrap(data, 0, data.length);
		ByteBuf slice = byteBuf.slice(1, 3);
		//[END REGION_4]
		System.out.println("Sliced byteBuf array: " + Arrays.toString(slice.asArray()));
		System.out.println();
	}

	private static void byteBufferConversion() {
		//[START REGION_5]
		ByteBuf byteBuf = ByteBuf.wrap(new byte[20], 0, 0);
		ByteBuffer buffer = byteBuf.toWriteByteBuffer();
		buffer.put((byte) 1);
		buffer.put((byte) 2);
		buffer.put((byte) 3);
		byteBuf.ofWriteByteBuffer(buffer);
		//[END REGION_5]
		System.out.println("Array of ByteBuf converted from ByteBuffer: " + Arrays.toString(byteBuf.asArray()));
		System.out.println();
	}

	public static void main(String[] args) {
		wrapForReading();
		wrapForWriting();
		stringConversion();
		slicing();
		byteBufferConversion();
	}
}
