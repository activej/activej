package adder;

import adder.AdderCommands.AddRequest;
import adder.AdderCommands.HasUserId;
import io.activej.common.exception.MalformedDataException;
import io.activej.config.Config;
import io.activej.crdt.CrdtException;
import io.activej.crdt.storage.cluster.AsyncDiscoveryService;
import io.activej.crdt.storage.cluster.FileDiscoveryService;
import io.activej.crdt.storage.cluster.PartitionId;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launchers.crdt.rpc.CrdtRpcClientLauncher;
import io.activej.launchers.crdt.rpc.CrdtRpcStrategyService;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.client.ReactiveRpcClient;
import io.activej.rpc.client.AsyncRpcClient;

import java.nio.file.Path;
import java.util.List;
import java.util.Scanner;

import static adder.AdderCommands.GetRequest;
import static adder.AdderCommands.GetResponse;
import static adder.AdderServerLauncher.MESSAGE_TYPES;
import static adder.ClusterStorageModule.DEFAULT_PARTITIONS_FILE;
import static io.activej.common.Checks.checkNotNull;
import static io.activej.config.converter.ConfigConverters.ofPath;
import static io.activej.rpc.client.sender.RpcStrategies.server;

public final class AdderClientLauncher extends CrdtRpcClientLauncher {
	@Inject
	Reactor reactor;

	@Inject
	AsyncRpcClient client;

	@Override
	protected List<Class<?>> getMessageTypes() {
		return MESSAGE_TYPES;
	}

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			AsyncRpcClient client(NioReactor reactor, CrdtRpcStrategyService<Long> strategyService, List<Class<?>> messageTypes) {
				ReactiveRpcClient rpcClient = ReactiveRpcClient.create(reactor)
						.withMessageTypes(messageTypes);
				strategyService.setRpcClient(rpcClient);
				return rpcClient;
			}
		};
	}

	@Provides
	AsyncDiscoveryService<PartitionId> discoveryServiceDiscoveryService(Reactor reactor, Config config) throws CrdtException {
		Path pathToFile = config.get(ofPath(), "crdt.cluster.partitionFile", DEFAULT_PARTITIONS_FILE);
		return FileDiscoveryService.create(reactor, pathToFile)
				.withRpcProvider(partitionId -> server(checkNotNull(partitionId.getRpcAddress())));
	}

	@Provides
	CrdtRpcStrategyService<Long> rpcStrategyService(
			Reactor reactor,
			AsyncDiscoveryService<PartitionId> discoveryService
	) {
		return CrdtRpcStrategyService.create(reactor, discoveryService, AdderClientLauncher::extractKey);
	}

	@Override
	protected void run() throws Exception {
		System.out.println("Available commands:");
		System.out.println("->\tadd <long id> <float delta>");
		System.out.println("->\tget <long id>\n");

		Scanner scanIn = new Scanner(System.in);
		while (true) {
			System.out.print("> ");
			String line = scanIn.nextLine().trim();
			if (line.isEmpty()) {
				shutdown();
				return;
			}
			String[] parts = line.split("\\s+");
			try {
				if (parts[0].equalsIgnoreCase("add")) {
					if (parts.length != 3) {
						throw new MalformedDataException("3 parts expected");
					}
					long id = Long.parseLong(parts[1]);
					float value = Float.parseFloat(parts[2]);
					reactor.submit(() -> client.sendRequest(new AddRequest(id, value))).get();
					System.out.println("---> OK");
				} else if (parts[0].equalsIgnoreCase("get")) {
					if (parts.length != 2) {
						throw new MalformedDataException("2 parts expected");
					}
					long id = Long.parseLong(parts[1]);
					GetResponse getResponse = reactor.submit(() -> client.
							<GetRequest, GetResponse>sendRequest(new GetRequest(id))).get();
					System.out.println("---> " + getResponse.sum());
				} else {
					throw new MalformedDataException("Unknown command: " + parts[0]);
				}
			} catch (MalformedDataException | NumberFormatException e) {
				logger.warn("Invalid input: {}", e.getMessage());
			}
		}
	}

	private static long extractKey(Object request) {
		if (request instanceof HasUserId) return ((HasUserId) request).userId();
		throw new IllegalArgumentException();
	}

	public static void main(String[] args) throws Exception {
		new AdderClientLauncher().launch(args);
	}
}
