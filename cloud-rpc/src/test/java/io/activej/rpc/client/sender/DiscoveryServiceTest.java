package io.activej.rpc.client.sender;

import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.hash.ShardingFunction;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static io.activej.test.TestUtils.getFreePort;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("BusyWait")
public final class DiscoveryServiceTest {
	private static final int NUMBER_OF_SERVERS = 5;

	private Thread thread;

	private final int[] ports = new int[NUMBER_OF_SERVERS];
	private final Map<String, RpcServer> servers = new HashMap<>();
	private final Map<String, List<String>> serverStorages = new ConcurrentHashMap<>();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Before
	public void setUp() throws Exception {
		Eventloop eventloop = Eventloop.getCurrentEventloop();

		for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
			int port = getFreePort();
			String serverId = "server_" + i;

			RpcServer server = RpcServer.create(eventloop)
					.withMessageTypes(SimpleRequest.class, VoidResponse.class)
					.withHandler(SimpleRequest.class, request -> {
						serverStorages.computeIfAbsent(serverId, $ -> new ArrayList<>()).add(request.data);
						return Promise.of(new VoidResponse());
					})
					.withListenPort(port);

			ports[i] = port;
			servers.put(serverId, server);
			server.listen();
		}

		thread = new Thread(eventloop);
		thread.start();
	}

	@After
	public void tearDown() throws InterruptedException {
		if (thread != null) {
			thread.join();
		}
	}

	@Test
	public void roundRobbin() throws Exception {
		Map<Object, InetSocketAddress> firstPartitions = new LinkedHashMap<>();
		firstPartitions.put("server_0", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[0]));
		firstPartitions.put("server_1", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[1]));

		Map<Object, InetSocketAddress> secondPartitions = new LinkedHashMap<>();
		secondPartitions.put("server_2", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[2]));
		secondPartitions.put("server_3", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[3]));
		secondPartitions.put("server_4", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[4]));

		SettableDiscoveryService discoveryService = SettableDiscoveryService.create(firstPartitions);
		RpcStrategyList rpcStrategyList = RpcStrategyList.ofDiscoveryService(discoveryService);

		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(SimpleRequest.class, VoidResponse.class)
				.withStrategy(RpcStrategies.roundRobin(rpcStrategyList).withMinActiveSubStrategies(2));

		client.startFuture().get();

		for (int i = 0; i < 10; i++) {
			request(client, "first_" + i);
		}

		// set new addresses
		client.getEventloop().submit(() -> discoveryService.setAddressMap(secondPartitions)).get();

		// wait for successful connections
		while (client.getActiveConnections() != rpcStrategyList.size()) {
			Thread.sleep(1);
		}

		for (int i = 0; i < 10; i++) {
			request(client, "second_" + i);
		}

		assertEquals(asList("first_1", "first_3", "first_5", "first_7", "first_9"), serverStorages.get("server_0"));
		assertEquals(asList("first_0", "first_2", "first_4", "first_6", "first_8"), serverStorages.get("server_1"));

		assertEquals(asList("second_0", "second_3", "second_6", "second_9"), serverStorages.get("server_2"));
		assertEquals(asList("second_1", "second_4", "second_7"), serverStorages.get("server_3"));
		assertEquals(asList("second_2", "second_5", "second_8"), serverStorages.get("server_4"));
		client.stopFuture().get();

		for (RpcServer server : servers.values()) {
			server.closeFuture().get();
		}
	}

	@Test
	public void sharding() throws Exception {
		Map<Object, InetSocketAddress> firstPartitions = new LinkedHashMap<>();
		firstPartitions.put("server_0", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[0]));
		firstPartitions.put("server_1", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[1]));

		Map<Object, InetSocketAddress> secondPartitions = new LinkedHashMap<>();
		secondPartitions.put("server_2", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[2]));
		secondPartitions.put("server_3", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[3]));
		secondPartitions.put("server_4", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[4]));

		SettableDiscoveryService discoveryService = SettableDiscoveryService.create(firstPartitions);
		RpcStrategyList rpcStrategyList = RpcStrategyList.ofDiscoveryService(discoveryService);

		ShardingFunction<SimpleRequest> shardingFunction = item -> Math.abs(item.data.hashCode() % rpcStrategyList.size());

		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(SimpleRequest.class, VoidResponse.class)
				.withStrategy(RpcStrategies.sharding(shardingFunction, rpcStrategyList).withMinActiveSubStrategies(2));

		client.startFuture().get();

		for (int i = 0; i < 10; i++) {
			request(client, "first_" + i);
		}

		// set new addresses
		client.getEventloop().submit(() -> discoveryService.setAddressMap(secondPartitions)).get();

		// wait for successful connections
		while (client.getActiveConnections() != rpcStrategyList.size()) {
			Thread.sleep(1);
		}

		for (int i = 0; i < 10; i++) {
			request(client, "second_" + i);
		}

		assertEquals(asList("first_0", "first_2", "first_4", "first_6", "first_8"), serverStorages.get("server_0"));
		assertEquals(asList("first_1", "first_3", "first_5", "first_7", "first_9"), serverStorages.get("server_1"));

		assertEquals(asList("second_0", "second_3", "second_6", "second_9"), serverStorages.get("server_2"));
		assertEquals(asList("second_1", "second_4", "second_7"), serverStorages.get("server_3"));
		assertEquals(asList("second_2", "second_5", "second_8"), serverStorages.get("server_4"));
		client.stopFuture().get();

		for (RpcServer server : servers.values()) {
			server.closeFuture().get();
		}
	}

	@Test
	public void rendezvousHashing() throws Exception {
		Map<Object, InetSocketAddress> firstPartitions = new LinkedHashMap<>();
		firstPartitions.put("storage_0", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[0]));
		firstPartitions.put("storage_1", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[1]));
		firstPartitions.put("storage_2", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[2]));

		Map<Object, InetSocketAddress> secondPartitions = new LinkedHashMap<>();
		secondPartitions.put("server_0", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[0]));
		secondPartitions.put("server_1", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[1]));
		secondPartitions.put("server_3", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[3]));
		secondPartitions.put("server_4", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[4]));

		SettableDiscoveryService discoveryService = SettableDiscoveryService.create(firstPartitions);
		RpcStrategyList rpcStrategyList = RpcStrategyList.ofDiscoveryService(discoveryService);

		RpcStrategyRendezvousHashing original = RpcStrategies.rendezvousHashing(item -> ((SimpleRequest) item).data.hashCode());

		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(SimpleRequest.class, VoidResponse.class)
				.withStrategy(RpcStrategies.rendezvousHashing(discoveryService, original));

		client.startFuture().get();

		for (int i = 0; i < 100; i++) {
			request(client, "first_" + i);
		}

		// set new addresses
		client.getEventloop().submit(() -> discoveryService.setAddressMap(secondPartitions)).get();

		// wait for successful connections
		while (client.getActiveConnections() != rpcStrategyList.size()) {
			Thread.sleep(1);
		}

		for (int i = 0; i < 100; i++) {
			request(client, "second_" + i);
		}

		assertStartsWithBoth(serverStorages.get("server_0"));
		assertStartsWithBoth(serverStorages.get("server_1"));

		assertTrue(serverStorages.get("server_2").stream().allMatch(data -> data.startsWith("first")));
		assertTrue(serverStorages.get("server_3").stream().allMatch(data -> data.startsWith("second")));
		assertTrue(serverStorages.get("server_4").stream().allMatch(data -> data.startsWith("second")));

		client.stopFuture().get();

		for (RpcServer server : servers.values()) {
			server.closeFuture().get();
		}
	}

	@Test
	public void rendezvousHashingReplaceServer() throws Exception {
		Map<Object, InetSocketAddress> firstPartitions = new LinkedHashMap<>();
		firstPartitions.put("first", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[0]));
		firstPartitions.put("second", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[1]));

		Map<Object, InetSocketAddress> secondPartitions = new LinkedHashMap<>();
		secondPartitions.put("first", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[0]));
		secondPartitions.put("second", new InetSocketAddress(InetAddress.getByName("127.0.0.1"), ports[2]));

		SettableDiscoveryService discoveryService = SettableDiscoveryService.create(firstPartitions);
		RpcStrategyList rpcStrategyList = RpcStrategyList.ofDiscoveryService(discoveryService);

		RpcStrategyRendezvousHashing original = RpcStrategies.rendezvousHashing(item -> ((SimpleRequest) item).data.hashCode());

		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(SimpleRequest.class, VoidResponse.class)
				.withStrategy(RpcStrategies.rendezvousHashing(discoveryService, original));

		client.startFuture().get();

		for (int i = 0; i < 100; i++) {
			request(client, "data_" + i);
		}

		// set new addresses
		client.getEventloop().submit(() -> discoveryService.setAddressMap(secondPartitions)).get();

		// wait for successful connections
		while (client.getActiveConnections() != rpcStrategyList.size()) {
			Thread.sleep(1);
		}

		for (int i = 0; i < 100; i++) {
			request(client, "data_" + i);
		}

		List<String> storage_0 = serverStorages.get("server_0");
		List<String> storage_0_1 = storage_0.subList(0, storage_0.size() / 2);
		List<String> storage_0_2 = storage_0.subList(storage_0.size() / 2, storage_0.size());
		assertEquals(storage_0_1, storage_0_2);

		assertEquals(serverStorages.get("server_1"), serverStorages.get("server_2"));

		client.stopFuture().get();

		for (RpcServer server : servers.values()) {
			server.closeFuture().get();
		}
	}

	private static void assertStartsWithBoth(List<String> storage) {
		boolean startsWithPrefix1 = false;
		boolean startsWithPrefix2 = false;
		for (String data : storage) {
			if (data.startsWith("first")){
				startsWithPrefix1 = true;
			}
			if (data.startsWith("second")){
				startsWithPrefix2 = true;
			}
			if (startsWithPrefix1 && startsWithPrefix2) return;
		}
		Assert.fail();
	}

	private static void request(RpcClient rpcClient, String data) throws Exception {
		try {
			rpcClient.getEventloop().submit(
					() -> rpcClient.<SimpleRequest, Void>sendRequest(new SimpleRequest(data)))
					.get();
		} catch (ExecutionException e) {
			//noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException - cause is rethrown
			throw (Exception) e.getCause();
		}
	}

	public static final class SimpleRequest {
		@Serialize
		public final String data;

		public SimpleRequest(@Deserialize("data") String data) {
			this.data = data;
		}
	}

	public static final class VoidResponse {
	}
}
