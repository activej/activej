package io.activej.datastream.csp;

import io.activej.common.MemSize;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.serializer.BinarySerializers;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public final class ChannelSerializerDeserializerTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void initialBufferSizeOne() {
		List<Integer> ints = List.of(123, -567);

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(StreamSupplier.ofIterable(ints)
				.transformWith(ChannelSerializer.create(BinarySerializers.INT_SERIALIZER).withInitialBufferSize(MemSize.bytes(1)))
				.transformWith(ChannelDeserializer.create(BinarySerializers.INT_SERIALIZER))
				.streamTo(consumer));

		assertEquals(ints, consumer.getList());
	}

	@Test
	public void largeMessageSize() {
		int nearMaxSize = (1 << 28) // ChannelSerializer.MAX_SIZE
				- 4 // encoded size of an array
				- 1;
		List<byte[]> byteArrays = List.of(new byte[1024], new byte[32 * 1024], new byte[10 * 1024 * 1024], new byte[nearMaxSize]);
		for (byte[] byteArray : byteArrays) {
			ThreadLocalRandom.current().nextBytes(byteArray);
		}

		StreamConsumerToList<byte[]> consumer = StreamConsumerToList.create();

		await(StreamSupplier.ofIterable(byteArrays)
				.transformWith(ChannelSerializer.create(BinarySerializers.BYTES_SERIALIZER)
						.withInitialBufferSize(MemSize.bytes(1)))
				.transformWith(ChannelDeserializer.create(BinarySerializers.BYTES_SERIALIZER))
				.streamTo(consumer));

		List<byte[]> deserialized = consumer.getList();
		for (int i = 0; i < deserialized.size(); i++) {
			assertArrayEquals(byteArrays.get(i), deserialized.get(i));
		}
	}
}
