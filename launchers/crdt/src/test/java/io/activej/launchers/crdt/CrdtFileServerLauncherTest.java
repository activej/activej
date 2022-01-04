package io.activej.launchers.crdt;

import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.TimestampContainer;
import io.activej.inject.annotation.Provides;
import io.activej.test.rules.ByteBufRule;
import io.activej.types.TypeT;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;

public class CrdtFileServerLauncherTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testInjector() {
		new CrdtFileServerLauncher<String, TimestampContainer<Integer>>() {
			@Override
			protected CrdtFileServerLogicModule<String, TimestampContainer<Integer>> getBusinessLogicModule() {
				return new CrdtFileServerLogicModule<String, TimestampContainer<Integer>>() {};
			}

			@Provides
			CrdtDescriptor<String, TimestampContainer<Integer>> descriptor() {
				return new CrdtDescriptor<>(
						TimestampContainer.createCrdtFunction(Integer::max),
						new CrdtDataSerializer<>(UTF8_SERIALIZER,
								TimestampContainer.createSerializer(INT_SERIALIZER)),
						String.class,
						new TypeT<TimestampContainer<Integer>>() {}.getType());
			}
		}.testInjector();
	}
}
