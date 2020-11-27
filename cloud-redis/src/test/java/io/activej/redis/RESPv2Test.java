package io.activej.redis;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.MemSize;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.csp.process.ChannelByteChunker;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public final class RESPv2Test {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	public static final byte[] CR_LF = {13, 10};
	public static final Random RANDOM = ThreadLocalRandom.current();

	@Test
	public void nil() {
		test(RedisResponse.nil());
	}

	@Test
	public void string() {
		test(RedisResponse.string(""));
		test(RedisResponse.string("hello"));
		test(RedisResponse.string("hello " + '\r' + "world"));
		test(RedisResponse.string("hello " + '\n' + "world"));
	}

	@Test
	public void bytes() {
		test(RedisResponse.bytes("".getBytes()));
		test(RedisResponse.bytes("hello".getBytes()));
		test(RedisResponse.bytes("hello \r\n world".getBytes()));
	}

	@Test
	public void integer() {
		test(RedisResponse.integer(0));
		test(RedisResponse.integer(Long.MIN_VALUE));
		test(RedisResponse.integer(Long.MAX_VALUE));
	}

	@Test
	public void error() {
		test(RedisResponse.error(new ServerError("")));
		test(RedisResponse.error(new ServerError("ERROR")));
		test(RedisResponse.error(new ServerError("ERROR \n something went wrong")));
		test(RedisResponse.error(new ServerError("WARNING \r something went wrong")));
	}

	@Test
	public void array() {
		test(RedisResponse.array(emptyList()));
		test(RedisResponse.array(singletonList(null)));
		test(RedisResponse.array(singletonList("")));
		test(RedisResponse.array(singletonList("test")));
		test(RedisResponse.array(singletonList(0L)));
		test(RedisResponse.array(asList("first", "second")));
		test(RedisResponse.array(asList("first", 2L, "third", 4L)));
		test(RedisResponse.array(asList("first", 2L, null, "third", 4L)));
	}

	@Test
	public void arrayOfArrays() {
		test(RedisResponse.array(
				singletonList(
						emptyList()
				)));

		test(RedisResponse.array(
				asList(
						emptyList(),
						emptyList()
				)));

		test(RedisResponse.array(
				asList(
						null,
						emptyList(),
						null)
		));

		test(RedisResponse.array(
				asList(
						singletonList("test1"),
						singletonList("test2"),
						singletonList("test3"))
		));

		test(RedisResponse.array(
				asList(
						"Hello",
						asList(
								null,
								singletonList("inner"),
								"test",
								100L,
								new byte[]{1,2,3}
						),
						null,
						asList(
								1L,
								"",
								"test",
								emptyList())
				)));

		test(RedisResponse.array(
				singletonList(
						singletonList(
								singletonList(
										singletonList(
												singletonList(
														singletonList("test")
												)
										)
								)
						)
				))
		);
	}

	private ByteBufsDecoder<RedisResponse> codec() {
		RESPv2 protocolCodec = new RESPv2(new ByteBufQueue(), UTF_8);
		return protocolCodec::tryDecode;
	}

	private void test(RedisResponse response) {
		doTest(response, MemSize.bytes(1));
		doTest(response, MemSize.bytes(RANDOM.nextInt(100) + 1));
	}

	private void doTest(RedisResponse response, MemSize chunkSize) {
		List<RedisResponse> result = await(BinaryChannelSupplier.of(ChannelSupplier.of(encode(response))
				.transformWith(ChannelByteChunker.create(chunkSize, chunkSize)))
				.parseStream(codec())
				.toList());
		assertEquals(1, result.size());
		assertEquals(response, result.get(0));
	}

	private static ByteBuf encode(RedisResponse response) {
		ByteBufQueue queue = new ByteBufQueue();
		if (response.isNil()) {
			doEncodeNil(queue);
		} else if (response.isInteger()) {
			doEncode(response.getInteger(), queue);
		} else if (response.isString()) {
			doEncode(response.getString(), queue);
		} else if (response.isBytes()) {
			doEncode(response.getBytes(), queue);
		} else if (response.isError()) {
			doEncode(response.getError(), queue);
		} else if (response.isArray()) {
			doEncode(response.getArray(), queue);
		} else {
			throw new AssertionError();
		}
		return queue.takeRemaining();
	}

	private static void doEncodeNil(ByteBufQueue queue) {
		boolean bulkNull = RANDOM.nextBoolean();
		byte[] bytes = {(byte) (bulkNull ? '$' : '*'), '-', '1', 13, 10};
		queue.add(ByteBuf.wrapForReading(bytes));
	}

	private static void doEncode(String string, ByteBufQueue queue) {
		queue.add(ByteBuf.wrapForReading(new byte[]{'+'}));
		doEncodeString(string, queue);
	}

	private static void doEncode(byte[] bytes, ByteBufQueue queue) {
		queue.add(ByteBuf.wrapForReading(new byte[]{'$'}));
		queue.add(ByteBuf.wrapForReading(String.valueOf(bytes.length).getBytes()));
		queue.add(ByteBuf.wrapForReading(CR_LF));
		queue.add(ByteBuf.wrapForReading(bytes));
		queue.add(ByteBuf.wrapForReading(CR_LF));
	}

	private static void doEncode(Long integer, ByteBufQueue queue) {
		queue.add(ByteBuf.wrapForReading(new byte[]{':'}));
		queue.add(ByteBuf.wrapForReading(String.valueOf(integer).getBytes()));
		queue.add(ByteBuf.wrapForReading(CR_LF));
	}

	private static void doEncode(List<?> array, ByteBufQueue queue) {
		queue.add(ByteBuf.wrapForReading(new byte[]{'*'}));
		queue.add(ByteBuf.wrapForReading(String.valueOf(array.size()).getBytes()));
		queue.add(ByteBuf.wrapForReading(CR_LF));
		for (Object o : array) {
			if (o == null) {
				doEncodeNil(queue);
			} else if (o instanceof Long) {
				doEncode((Long) o, queue);
			} else if (o instanceof String) {
				doEncode((String) o, queue);
			} else if (o instanceof byte[]) {
				doEncode((byte[]) o, queue);
			} else if (o instanceof ServerError) {
				doEncode((ServerError) o, queue);
			}else {
				assert o instanceof List;
				doEncode((List<?>) o, queue);
			}
		}
	}

	private static void doEncode(ServerError error, ByteBufQueue queue) {
		queue.add(ByteBuf.wrapForReading(new byte[]{'-'}));
		doEncodeString(error.getMessage(), queue);
	}

	private static void doEncodeString(String string, ByteBufQueue queue) {
		queue.add(ByteBuf.wrapForReading(string.getBytes()));
		queue.add(ByteBuf.wrapForReading(new byte[]{13, 10}));
	}
}
