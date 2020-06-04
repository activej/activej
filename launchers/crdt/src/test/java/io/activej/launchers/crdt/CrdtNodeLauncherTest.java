package io.activej.launchers.crdt;

import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.crdt.util.TimestampContainer;
import io.activej.di.annotation.Provides;
import io.activej.eventloop.Eventloop;
import io.activej.remotefs.FsClient;
import io.activej.remotefs.LocalFsClient;
import org.junit.Test;

import java.nio.file.Paths;

import static io.activej.codec.StructuredCodecs.*;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class CrdtNodeLauncherTest {
	@Test
	public void testInjector() {
		new CrdtNodeLauncher<String, TimestampContainer<Integer>>() {
			@Override
			protected CrdtNodeLogicModule<String, TimestampContainer<Integer>> getBusinessLogicModule() {
				return new CrdtNodeLogicModule<String, TimestampContainer<Integer>>() {
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

					@Provides
					FsClient fsClient() {
						return LocalFsClient.create(Eventloop.create(), newSingleThreadExecutor(), Paths.get(""));
					}
				};
			}
		}.testInjector();
	}
}
