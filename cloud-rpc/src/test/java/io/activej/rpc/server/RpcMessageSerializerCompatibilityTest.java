package io.activej.rpc.server;

import io.activej.codegen.DefiningClassLoader;
import io.activej.rpc.protocol.RpcControlMessage;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.rpc.protocol.RpcRemoteException;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerFactory;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.annotations.SerializeVarLength;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.*;

public class RpcMessageSerializerCompatibilityTest {
	private static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();

	private static BinarySerializer<RpcMessage> oldSerializer;
	private static BinarySerializer<RpcMessage> newSerializer;

	@BeforeClass
	public static void beforeClass() throws Exception {
		oldSerializer = SerializerFactory.builder()
				.withSubclasses(RpcMessage.MESSAGE_TYPES, List.of(Request1.class, Request2.class, Response1.class, Response2.class))
				.build()
				.create(DefiningClassLoader.builder().withDebugOutputDir(Paths.get("/tmp/temp")).build(), RpcMessage.class);

		SerializerFactory serializerFactory = SerializerFactory.defaultInstance();

		LinkedHashMap<Class<?>, BinarySerializer<?>> serializers = new LinkedHashMap<>();
		serializers.put(Request1.class, serializerFactory.create(CLASS_LOADER, Request1.class));
		serializers.put(Request2.class, serializerFactory.create(CLASS_LOADER, Request2.class));
		serializers.put(Response1.class, serializerFactory.create(CLASS_LOADER, Response1.class));
		serializers.put(Response2.class, serializerFactory.create(CLASS_LOADER, Response2.class));

		newSerializer = new RpcMessageSerializer(serializers);
	}

	@Test
	public void testCompatibility() {
		doTest(RpcMessage.of(1, new Request1(100, "Hello")));
		doTest(RpcMessage.of(-1, new Request1(0, null)));

		doTest(RpcMessage.of(0, new Request2(200, "World")));
		doTest(RpcMessage.of(-200, new Request2(-2, null)));

		doTest(RpcMessage.of(123, new Response1(540, "Hi")));
		doTest(RpcMessage.of(-412421, new Response1(-434, null)));

		doTest(RpcMessage.of(3, new Response1(52341, "Everyone")));
		doTest(RpcMessage.of(0, new Response1(41242, null)));

		doTest(RpcMessage.of(1, RpcControlMessage.PING));
		doTest(RpcMessage.of(1, RpcControlMessage.CLOSE));
		doTest(RpcMessage.of(1, RpcControlMessage.PONG));

		doTest(RpcMessage.of(1, new RpcRemoteException(new Exception("Test"))));
		doTest(RpcMessage.of(1, new RpcRemoteException(new Exception("Test", new Exception("Cause")))));
		doTest(RpcMessage.of(1, new RpcRemoteException("Error")));
		doTest(RpcMessage.of(1, new RpcRemoteException("Error", "CauseClass", "Cause")));
		doTest(RpcMessage.of(1, new RpcRemoteException("Error", null, "Cause")));
		doTest(RpcMessage.of(1, new RpcRemoteException("Error", "CauseClass", null)));
		doTest(RpcMessage.of(1, new RpcRemoteException("Error", null, null)));

		doTest(RpcMessage.of(1, null));
	}

	private static void doTest(RpcMessage message) {
		byte[] buffer = new byte[1000];
		oldSerializer.encode(buffer, 0, message);
		assertMessage(message, newSerializer.decode(buffer, 0));

		newSerializer.encode(buffer, 0, message);
		assertMessage(message, oldSerializer.decode(buffer, 0));
	}

	private static void assertMessage(RpcMessage expected, RpcMessage actual) {
		assertEquals(expected.getCookie(), actual.getCookie());
		Object data = expected.getData();
		if (data == null) {
			assertNull(actual.getData());
		}

		if (data instanceof RpcRemoteException expectedEx) {
			Object actualData = actual.getData();
			if (!(actualData instanceof RpcRemoteException actualEx)) {
				fail();
				return;
			}

			assertEquals(expectedEx.getMessage(), actualEx.getMessage());
			assertEquals(expectedEx.getCauseMessage(), actualEx.getCauseMessage());
			assertEquals(expectedEx.getCauseClassName(), actualEx.getCauseClassName());
		}
	}

	// region messages
	public static final class Request1 {
		@Serialize
		public final int x;
		@Serialize
		@SerializeNullable
		public final String message;

		public Request1(@Deserialize("x") int x, @Deserialize("message") String message) {
			this.x = x;
			this.message = message;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Request1 request1 = (Request1) o;
			return x == request1.x && Objects.equals(message, request1.message);
		}

		@Override
		public int hashCode() {
			return Objects.hash(x, message);
		}
	}

	public static final class Request2 {
		@Serialize
		public final int x;
		@Serialize
		@SerializeNullable
		public final String message;

		public Request2(@Deserialize("x") int x, @Deserialize("message") String message) {
			this.x = x;
			this.message = message;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Request2 request2 = (Request2) o;
			return x == request2.x && Objects.equals(message, request2.message);
		}

		@Override
		public int hashCode() {
			return Objects.hash(x, message);
		}
	}

	public static final class Response1 {
		@Serialize
		@SerializeVarLength
		public final int x;
		@Serialize
		@SerializeNullable
		public final String message;

		public Response1(@Deserialize("x") int x, @Deserialize("message") String message) {
			this.x = x;
			this.message = message;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Response1 response1 = (Response1) o;
			return x == response1.x && Objects.equals(message, response1.message);
		}

		@Override
		public int hashCode() {
			return Objects.hash(x, message);
		}
	}

	public static final class Response2 {
		@Serialize
		public final int x;
		@Serialize
		@SerializeNullable
		public final String message;

		public Response2(@Deserialize("x") int x, @Deserialize("message") String message) {
			this.x = x;
			this.message = message;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Response2 response2 = (Response2) o;
			return x == response2.x && Objects.equals(message, response2.message);
		}

		@Override
		public int hashCode() {
			return Objects.hash(x, message);
		}
	}
	// endregion
}
