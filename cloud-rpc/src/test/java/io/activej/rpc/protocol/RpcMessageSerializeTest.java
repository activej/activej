package io.activej.rpc.protocol;

import io.activej.codegen.DefiningClassLoader;
import io.activej.rpc.server.RpcMessageSerializer;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerFactory;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
		RpcMessage message1 = RpcMessage.of(1, messageData1);
		BinarySerializer<TestRpcMessageData> testRpcMessageDataSerializer = SerializerFactory.defaultInstance()
				.create(DefiningClassLoader.create(), TestRpcMessageData.class);

		LinkedHashMap<Class<?>, BinarySerializer<?>> serializersMap = new LinkedHashMap<>();
		serializersMap.put(TestRpcMessageData.class, testRpcMessageDataSerializer);
		BinarySerializer<RpcMessage> serializer = new RpcMessageSerializer(serializersMap);

		byte[] buf = new byte[1000];
		serializer.encode(buf, 0, message1);
		RpcMessage message2 = serializer.decode(buf, 0);
		assertEquals(message1.getCookie(), message2.getCookie());
		assertTrue(message2.getData() instanceof TestRpcMessageData);
		TestRpcMessageData messageData2 = (TestRpcMessageData) message2.getData();
		assertEquals(messageData1.s, messageData2.s);
	}
}
