package io.activej.launchers.crdt;

import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.inject.annotation.Provides;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;

public class CrdtFileServerLauncherTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testInjector() {
		new CrdtFileServerLauncher<String, Integer>() {
			@Override
			protected CrdtFileServerLogicModule<String, Integer> getBusinessLogicModule() {
				return new CrdtFileServerLogicModule<String, Integer>() {};
			}

			@Provides
			CrdtDescriptor<String, Integer> descriptor() {
				return new CrdtDescriptor<>(
						CrdtFunction.ignoringTimestamp(Integer::max),
						new CrdtDataSerializer<>(UTF8_SERIALIZER, INT_SERIALIZER),
						String.class,
						Integer.class
				);
			}
		}.testInjector();
	}
}
