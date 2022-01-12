package adder;

import adder.AdderCommands.AddRequest;
import adder.AdderCommands.HasUserId;
import discovery.RpcStrategyService;
import io.activej.common.exception.MalformedDataException;
import io.activej.config.Config;
import io.activej.crdt.storage.cluster.DiscoveryService;
import io.activej.crdt.storage.cluster.SimplePartitionId;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launchers.crdt.rpc.CrdtRpcClientLauncher;
import io.activej.rpc.client.RpcClient;

import java.util.List;
import java.util.Scanner;

import static adder.AdderCommands.GetRequest;
import static adder.AdderCommands.GetResponse;
import static adder.AdderServerLauncher.MESSAGE_TYPES;
import static io.activej.launchers.crdt.ConfigConverters.ofDiscoveryService;
import static io.activej.rpc.client.sender.RpcStrategies.server;

public final class AdderClientLauncher extends CrdtRpcClientLauncher {
	@Inject
	Eventloop eventloop;

	@Inject
	RpcClient client;

	@Override
	protected List<Class<?>> getMessageTypes() {
		return MESSAGE_TYPES;
	}

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			RpcClient client(Eventloop eventloop, RpcStrategyService<Long, DetailedSumsCrdtState, SimplePartitionId> strategyService, List<Class<?>> messageTypes) {
				RpcClient rpcClient = RpcClient.create(eventloop)
						.withMessageTypes(messageTypes);
				strategyService.setRpcClient(rpcClient);
				return rpcClient;
			}
		};
	}

	@Provides
	DiscoveryService<Long, DetailedSumsCrdtState, SimplePartitionId> discoveryService(Eventloop eventloop, Config config) {
		return config.get(ofDiscoveryService(), "crdt.cluster");
	}

	@Provides
	RpcStrategyService<Long, DetailedSumsCrdtState, SimplePartitionId> rpcStrategyService(
			Eventloop eventloop,
			DiscoveryService<Long, DetailedSumsCrdtState, SimplePartitionId> discoveryService
	) {
		return RpcStrategyService.create(eventloop, discoveryService, partitionId -> server(partitionId.getRpcAddress()), AdderClientLauncher::extractKey);
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
					eventloop.submit(() -> client.sendRequest(new AddRequest(id, value))).get();
					System.out.println("---> OK");
				} else if (parts[0].equalsIgnoreCase("get")) {
					if (parts.length != 2) {
						throw new MalformedDataException("2 parts expected");
					}
					long id = Long.parseLong(parts[1]);
					GetResponse getResponse = eventloop.submit(() -> client.
							<GetRequest, GetResponse>sendRequest(new GetRequest(id))).get();
					System.out.println("---> " + getResponse.getSum());
				} else {
					throw new MalformedDataException("Unknown command: " + parts[0]);
				}
			} catch (MalformedDataException | NumberFormatException e) {
				logger.warn("Invalid input: {}", e.getMessage());
			}
		}
	}

	private static long extractKey(Object request) {
		if (request instanceof HasUserId) return ((HasUserId) request).getUserId();
		throw new IllegalArgumentException();
	}

	public static void main(String[] args) throws Exception {
		new AdderClientLauncher().launch(args);
	}
}
