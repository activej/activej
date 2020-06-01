package io.activej.launchers.crdt;

import io.activej.crdt.CrdtDataSerializer;
import io.activej.crdt.TimestampContainer;
import io.activej.di.annotation.Provides;
import org.junit.Test;

import static io.activej.codec.StructuredCodecs.*;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;

public class CrdtFileServerLauncherTest {
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
						STRING_CODEC,
						tuple(TimestampContainer::new,
								TimestampContainer::getTimestamp, LONG_CODEC,
								TimestampContainer::getState, INT_CODEC));
			}
		}.testInjector();
	}
}
