package ${groupId};

import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.client.IRpcClient;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.RpcStrategies;
import io.activej.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.util.Scanner;

import static io.activej.config.converter.ConfigConverters.ofInteger;

@SuppressWarnings({"WeakerAccess", "unused"})
public class MyRpcClient extends Launcher {
    private static final int RPC_LISTENER_PORT = 5353;

    @Inject
    Reactor reactor;

    @Inject
    IRpcClient client;

    @Provides
    Config config() {
        return Config.ofProperties(System.getProperties()).getChild("config");
    }

    @Provides
    NioReactor reactor() {
        return Eventloop.create();
    }

    @Provides
    IRpcClient rpcClient(NioReactor reactor, Config config) {
        return RpcClient.builder(reactor)
            .withMessageTypes(String.class)
            .withStrategy(RpcStrategies.server(
                new InetSocketAddress(config.get(ofInteger(), "port", RPC_LISTENER_PORT))))
            .build();
    }

    @Override
    protected Module getModule() {
        return ServiceGraphModule.create();
    }

    @Override
    protected void run() throws Exception {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Your input > ");
            String line = scanner.nextLine();
            if (line.equalsIgnoreCase("exit")) {
                return;
            }
            reactor.submit(() -> client.sendRequest(line))
                .thenAccept(string -> System.out.println("Response: " + string + "\n"))
                .get();
        }
    }

    public static void main(String[] args) throws Exception {
        MyRpcClient client = new MyRpcClient();
        client.launch(args);
    }
}
