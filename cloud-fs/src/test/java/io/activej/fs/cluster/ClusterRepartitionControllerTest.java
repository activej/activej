package io.activej.fs.cluster;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelConsumer;
import io.activej.fs.FileSystem;
import io.activej.fs.ForwardingFileSystem;
import io.activej.fs.IFileSystem;
import io.activej.fs.tcp.FileSystemServer;
import io.activej.fs.tcp.FileSystem_Remote;
import io.activej.net.AbstractReactiveServer;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.TestUtils;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.fs.cluster.ServerSelector.RENDEZVOUS_HASH_SHARDER;
import static io.activej.fs.util.RemoteFileSystemUtils.ofFixedSize;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.*;

public final class ClusterRepartitionControllerTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Test
	public void testFailingUploadDoesNotMarkAllPartitionsDead() throws IOException {
		NioReactor reactor = Reactor.getCurrentReactor();

		Executor executor = Executors.newSingleThreadExecutor();
		List<FileSystemServer> servers = new ArrayList<>();
		Map<Object, IFileSystem> partitions = new HashMap<>();

		Path storage = tmpFolder.newFolder().toPath();
		Path localStorage = storage.resolve("local");
		Files.createDirectories(localStorage);

		// 50 MB file
		Path filePath = localStorage.resolve("file");
		Files.createFile(filePath);
		RandomAccessFile file = new RandomAccessFile(filePath.toString(), "rw");
		int fileSize = 50 * 1024 * 1024;
		file.setLength(fileSize);
		file.close();

		FileSystem localFileSystem = FileSystem.create(reactor, executor, localStorage);
		await(localFileSystem.start());

		Object localPartitionId = "local";
		partitions.put(localPartitionId, localFileSystem);

		InetSocketAddress regularPartitionAddress = new InetSocketAddress("localhost", getFreePort());
		Path regularPath = storage.resolve("regular");
		Files.createDirectories(regularPath);
		partitions.put("regular", FileSystem_Remote.create(reactor, regularPartitionAddress));
		FileSystem fileSystem = FileSystem.create(reactor, executor, regularPath);
		await(fileSystem.start());

		InetSocketAddress failingPartitionAddress = new InetSocketAddress("localhost", getFreePort());
		Path failingPath = storage.resolve("failing");
		Files.createDirectories(failingPath);
		partitions.put("failing", FileSystem_Remote.create(reactor, failingPartitionAddress));
		FileSystem peer = FileSystem.create(reactor, executor, failingPath);
		await(peer.start());

		FileSystemServer regularServer = FileSystemServer.builder(reactor, fileSystem)
				.withListenAddress(regularPartitionAddress)
				.build();
		regularServer.listen();
		servers.add(regularServer);

		FileSystemServer failingServer = FileSystemServer.builder(reactor,
						new ForwardingFileSystem(peer) {
							@Override
							public Promise<ChannelConsumer<ByteBuf>> upload(String name, long size) {
								return super.upload(name)
										.map(consumer -> consumer.transformWith(ofFixedSize(fileSize / 2)));
							}
						})
				.withListenAddress(failingPartitionAddress)
				.build();
		failingServer.listen();
		servers.add(failingServer);

		IDiscoveryService discoveryService = IDiscoveryService.constant(partitions);
		FileSystemPartitions fileSystemPartitions = FileSystemPartitions.builder(reactor, discoveryService)
				.withServerSelector(RENDEZVOUS_HASH_SHARDER)
				.build();

		Promise<Map<Object, IFileSystem>> discoverPromise = discoveryService.discover().get();
		Map<Object, IFileSystem> discovered = discoverPromise.getResult();

		ClusterRepartitionController controller = ClusterRepartitionController.builder(reactor, localPartitionId, fileSystemPartitions)
				.withReplicationCount(partitions.size())    // full replication
				.build();

		assertTrue(discovered.containsKey("regular"));
		assertTrue(discovered.containsKey("failing")); // no one has marked it dead yet

		await(fileSystemPartitions.start()
				.then(controller::start)
				.then(controller::repartition)
				.whenComplete(TestUtils.assertCompleteFn($ -> servers.forEach(AbstractReactiveServer::close))));

		assertTrue(fileSystemPartitions.getAlivePartitions().containsKey("regular"));
		assertFalse(fileSystemPartitions.getAlivePartitions().containsKey("failing"));

		assertEquals(fileSize, Files.size(localStorage.resolve("file")));
		assertEquals(fileSize, Files.size(regularPath.resolve("file")));
		assertFalse(Files.exists(failingPath.resolve("file")));
	}

}
