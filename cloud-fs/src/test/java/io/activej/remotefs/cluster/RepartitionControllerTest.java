package io.activej.remotefs.cluster;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.ref.RefInt;
import io.activej.csp.ChannelConsumer;
import io.activej.eventloop.Eventloop;
import io.activej.net.AbstractServer;
import io.activej.promise.Promise;
import io.activej.remotefs.*;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.NotNull;
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

import static io.activej.promise.TestUtils.await;
import static io.activej.remotefs.cluster.ServerSelector.RENDEZVOUS_HASH_SHARDER;
import static io.activej.test.TestUtils.assertComplete;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.*;

public final class RepartitionControllerTest {
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
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		Executor executor = Executors.newSingleThreadExecutor();
		List<RemoteFsServer> servers = new ArrayList<>();
		Map<Object, FsClient> clients = new HashMap<>();

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

		LocalFsClient localFsClient = LocalFsClient.create(eventloop, executor, localStorage);

		Object localPartitionId = "local";
		clients.put(localPartitionId, localFsClient);

		InetSocketAddress regularPartitionAddress = new InetSocketAddress("localhost", getFreePort());
		Path regularPath = storage.resolve("regular");
		Files.createDirectories(regularPath);
		clients.put("regular", RemoteFsClient.create(eventloop, regularPartitionAddress));
		RemoteFsServer regularServer = RemoteFsServer.create(eventloop, executor, regularPath).withListenAddress(regularPartitionAddress);
		regularServer.listen();
		servers.add(regularServer);

		InetSocketAddress failingPartitionAddress = new InetSocketAddress("localhost", getFreePort());
		Path failingPath = storage.resolve("failing");
		Files.createDirectories(failingPath);
		clients.put("failing", RemoteFsClient.create(eventloop, failingPartitionAddress));
		LocalFsClient peer = LocalFsClient.create(eventloop, executor, failingPath);
		RemoteFsServer failingServer = RemoteFsServer.create(eventloop,
				new ForwardingFsClient(peer) {
					@Override
					public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
						RefInt total = new RefInt(0);
						return super.upload(name)
								.map(consumer -> consumer.mapAsync(buf -> {
									total.inc(buf.readRemaining());
									if (total.get() > 5 * 1024 * 1024) {
										// file after 5 MB
										buf.recycle();
										return Promise.ofException(new Exception());
									}
									return Promise.of(buf);
								}));
					}
				})
				.withListenAddress(failingPartitionAddress);
		failingServer.listen();
		servers.add(failingServer);

		FsPartitions partitions = FsPartitions.create(eventloop, clients)
				.withServerSelector(RENDEZVOUS_HASH_SHARDER);

		RemoteFsRepartitionController controller = RemoteFsRepartitionController.create(localPartitionId, partitions)
				.withReplicationCount(clients.size());    // full replication

		assertTrue(partitions.getAliveClients().containsKey("regular"));
		assertTrue(partitions.getAliveClients().containsKey("failing")); // no one has marked it dead yet

		await(controller.repartition()
				.whenComplete(assertComplete($ -> servers.forEach(AbstractServer::close))));

		assertTrue(partitions.getAliveClients().containsKey("regular"));
		assertFalse(partitions.getAliveClients().containsKey("failing"));

		assertEquals(fileSize, Files.size(localStorage.resolve("file")));
		assertEquals(fileSize, Files.size(regularPath.resolve("file")));
		assertTrue(Files.size(failingPath.resolve("file")) < fileSize);
	}

}
