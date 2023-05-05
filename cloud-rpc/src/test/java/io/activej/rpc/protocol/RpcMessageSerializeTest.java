package io.activej.rpc.protocol;

import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerFactory;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

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
		RpcMessage message1 = new RpcMessage(1, messageData1);

		BinarySerializer<RpcMessage> serializer = SerializerFactory.builder()
				.withSubclasses(RpcMessage.SUBCLASSES_ID, List.of(TestRpcMessageData.class))
				.build()
				.create(DefiningClassLoader.create(), RpcMessage.class);

		byte[] buf = new byte[1000];
		serializer.encode(buf, 0, message1);
		RpcMessage message2 = serializer.decode(buf, 0);
		assertEquals(message1.getIndex(), message2.getIndex());
		assertTrue(message2.getMessage() instanceof TestRpcMessageData);
		TestRpcMessageData messageData2 = (TestRpcMessageData) message2.getMessage();
		assertEquals(messageData1.s, messageData2.s);
	}
}
