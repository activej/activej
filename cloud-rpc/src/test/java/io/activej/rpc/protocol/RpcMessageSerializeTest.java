package io.activej.rpc.protocol;

import io.activej.eventloop.Eventloop;
import io.activej.rpc.client.RpcClient;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerFactory;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.*;

public final class RpcMessageSerializeTest {

	@SerializeRecord
	public record TestRpcMessageData(String s) {}

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Test
	public void testRpcMessage() {
		TestRpcMessageData messageData1 = new TestRpcMessageData("TestMessageData");
		RpcMessage message1 = new RpcMessage(1, messageData1);
		BinarySerializer<TestRpcMessageData> testRpcMessageDataSerializer = SerializerFactory.defaultInstance()
				.create(TestRpcMessageData.class);

		LinkedHashMap<Class<?>, BinarySerializer<?>> serializersMap = new LinkedHashMap<>();
		serializersMap.put(TestRpcMessageData.class, testRpcMessageDataSerializer);
		BinarySerializer<RpcMessage> serializer = new RpcMessageSerializer(serializersMap);

		byte[] buf = new byte[1000];
		serializer.encode(buf, 0, message1);
		RpcMessage message2 = serializer.decode(buf, 0);
		assertEquals(message1.getIndex(), message2.getIndex());
		assertTrue(message2.getMessage() instanceof TestRpcMessageData);
		TestRpcMessageData messageData2 = (TestRpcMessageData) message2.getMessage();
		assertEquals(messageData1.s, messageData2.s);
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testDeprecatedMessageFormat() {
		RpcClient client = RpcClient.builder(Eventloop.create())
				.withSerializer(RpcMessageSerializer.ofV5(List.of(Data1.class, Data2.class)))
				.build();

		BinarySerializer<RpcMessage> requestSerializer = client.getRequestSerializer();
		BinarySerializer<RpcMessage> responseSerializer = client.getResponseSerializer();

		assertSame(requestSerializer, responseSerializer);

		byte[] bytes = new byte[1024];
		int encoded = requestSerializer.encode(bytes, 0, new RpcMessage(2, new Data1(7)));
		assertArrayEquals(
				new byte[]{
						5,    		// max version
						0, 0, 0, 2,	// index,
						2, 			// subclass index
						0, 0, 0, 7  // number
				},
					Arrays.copyOf(bytes, encoded));

		encoded = requestSerializer.encode(bytes, 0, new RpcMessage(9, new Data2("test")));
		assertArrayEquals(
				new byte[]{
						5,    				// max version
						0, 0, 0, 9,			// index,
						3, 					// subclass index
						4, 					// name length
						116, 101, 115, 116	// name
				},
				Arrays.copyOf(bytes, encoded));
	}

	public static final class Data1 {
		private final int number;

		public Data1(@Deserialize("number") int number) {
			this.number = number;
		}

		@Serialize(added = 3)
		public int getNumber() {
			return number;
		}
	}

	public static final class Data2 {
		private final String name;

		public Data2(@Deserialize("name") String name) {
			this.name = name;
		}

		@Serialize(added = 5)
		public String getName() {
			return name;
		}
	}

}
