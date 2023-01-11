package io.activej.fs.cluster;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelConsumer;
import io.activej.fs.AsyncFs;
import io.activej.fs.ForwardingFs;
import io.activej.fs.Fs_Local;
import io.activej.fs.tcp.FsServer;
import io.activej.fs.tcp.Fs_Remote;
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
import static io.activej.fs.util.RemoteFsUtils.ofFixedSize;
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
		List<FsServer> servers = new ArrayList<>();
		Map<Object, AsyncFs> partitions = new HashMap<>();

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

		Fs_Local localFsClient = Fs_Local.create(reactor, executor, localStorage);
		await(localFsClient.start());

		Object localPartitionId = "local";
		partitions.put(localPartitionId, localFsClient);

		InetSocketAddress regularPartitionAddress = new InetSocketAddress("localhost", getFreePort());
		Path regularPath = storage.resolve("regular");
		Files.createDirectories(regularPath);
		partitions.put("regular", Fs_Remote.create(reactor, regularPartitionAddress));
		Fs_Local localFs = Fs_Local.create(reactor, executor, regularPath);
		await(localFs.start());

		InetSocketAddress failingPartitionAddress = new InetSocketAddress("localhost", getFreePort());
		Path failingPath = storage.resolve("failing");
		Files.createDirectories(failingPath);
		partitions.put("failing", Fs_Remote.create(reactor, failingPartitionAddress));
		Fs_Local peer = Fs_Local.create(reactor, executor, failingPath);
		await(peer.start());

		FsServer regularServer = FsServer.create(reactor, localFs).withListenAddress(regularPartitionAddress);
		regularServer.listen();
		servers.add(regularServer);

		FsServer failingServer = FsServer.create(reactor,
						new ForwardingFs(peer) {
							@Override
							public Promise<ChannelConsumer<ByteBuf>> upload(String name, long size) {
								return super.upload(name)
										.map(consumer -> consumer.transformWith(ofFixedSize(fileSize / 2)));
							}
						})
				.withListenAddress(failingPartitionAddress);
		failingServer.listen();
		servers.add(failingServer);

		AsyncDiscoveryService discoveryService = AsyncDiscoveryService.constant(partitions);
		FsPartitions fsPartitions = FsPartitions.create(reactor, discoveryService)
				.withServerSelector(RENDEZVOUS_HASH_SHARDER);

		Promise<Map<Object, AsyncFs>> discoverPromise = discoveryService.discover().get();
		Map<Object, AsyncFs> discovered = discoverPromise.getResult();

		ClusterRepartitionController controller = ClusterRepartitionController.create(reactor, localPartitionId, fsPartitions)
				.withReplicationCount(partitions.size());    // full replication

		assertTrue(discovered.containsKey("regular"));
		assertTrue(discovered.containsKey("failing")); // no one has marked it dead yet

		await(fsPartitions.start()
				.then(controller::start)
				.then(controller::repartition)
				.whenComplete(TestUtils.assertCompleteFn($ -> servers.forEach(AbstractReactiveServer::close))));

		assertTrue(fsPartitions.getAlivePartitions().containsKey("regular"));
		assertFalse(fsPartitions.getAlivePartitions().containsKey("failing"));

		assertEquals(fileSize, Files.size(localStorage.resolve("file")));
		assertEquals(fileSize, Files.size(regularPath.resolve("file")));
		assertFalse(Files.exists(failingPath.resolve("file")));
	}

}
