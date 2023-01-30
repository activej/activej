package io.activej.launchers.crdt;

import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.crdt.util.CrdtDataBinarySerializer;
import io.activej.eventloop.Eventloop;
import io.activej.fs.FileSystem;
import io.activej.fs.IFileSystem;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.crdt.CrdtNodeLogicModule.Cluster;
import io.activej.launchers.crdt.CrdtNodeLogicModule.InMemory;
import io.activej.launchers.crdt.CrdtNodeLogicModule.Persistent;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.file.Paths;

import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class CrdtNodeLauncherTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testInjector() {
		new CrdtNodeLauncher<String, Integer>() {

			@Inject
			@InMemory
			ICrdtStorage<String, Integer> inMemory;

			@Inject
			@Persistent
			ICrdtStorage<String, Integer> persistent;

			@Inject
			@Cluster
			ICrdtStorage<String, Integer> cluster;

			@Override
			protected CrdtNodeLogicModule<String, Integer> getBusinessLogicModule() {
				return new CrdtNodeLogicModule<String, Integer>() {
					@Provides
					CrdtDescriptor<String, Integer> descriptor() {
						return new CrdtDescriptor<>(
								CrdtFunction.ignoringTimestamp(Integer::max),
								new CrdtDataBinarySerializer<>(UTF8_SERIALIZER, INT_SERIALIZER),
								String.class,
								Integer.class);
					}

					@Provides
					IFileSystem fileSystem() {
						return FileSystem.create(Eventloop.create(), newSingleThreadExecutor(), Paths.get(""));
					}
				};
			}
		}.testInjector();
	}
}
