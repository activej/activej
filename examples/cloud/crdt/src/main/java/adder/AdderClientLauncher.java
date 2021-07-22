package adder;

import adder.AdderCommands.PutRequest;
import io.activej.common.exception.MalformedDataException;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.crdt.rpc.CrdtRpcClientLauncher;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.hash.ShardingFunction;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import static adder.AdderCommands.GetRequest;
import static adder.AdderCommands.GetResponse;
import static adder.AdderServerLauncher.MESSAGE_TYPES;
import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.config.converter.ConfigConverters.ofList;

public final class AdderClientLauncher extends CrdtRpcClientLauncher {
	@Inject
	Eventloop eventloop;

	@Inject
	RpcClient client;

	@Override
	protected List<Class<?>> getMessageTypes() {
		return MESSAGE_TYPES;
	}

	@Provides
	ShardingFunction<?> shardingFunction(Config config) {
		List<InetSocketAddress> addresses = config.get(ofList(ofInetSocketAddress()), "addresses", Collections.emptyList());
		if (addresses.isEmpty()) {
			return $ -> {
				throw new IllegalStateException();
			};
		}

		int shardsCount = addresses.size();
		return item -> {
			if (item instanceof PutRequest) {
				return (int) (((PutRequest) item).getUserId() % shardsCount);
			}
			assert item instanceof GetRequest;
			return (int) (((GetRequest) item).getUserId() % shardsCount);
		};
	}

	@Override
	protected void run() throws Exception {
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
				if (parts[0].equalsIgnoreCase("put")) {
					if (parts.length != 3){
						throw new MalformedDataException("3 parts expected");
					}
					long id = Long.parseLong(parts[1]);
					float value = Float.parseFloat(parts[2]);
					eventloop.submit(() -> client.sendRequest(new PutRequest(id, value))).get();
					System.out.println("---> OK");
				} else if (parts[0].equalsIgnoreCase("get")) {
					if (parts.length != 2){
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

	public static void main(String[] args) throws Exception {
		new AdderClientLauncher().launch(args);
	}
}
