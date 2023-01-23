package ${groupId};

import io.activej.config.Config;
import io.activej.config.converter.ConfigConverters;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.rpc.RpcServerLauncher;
import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.server.RpcServer;

@SuppressWarnings("unused")
public class MyRpcServer extends RpcServerLauncher {
    private static final int RPC_SERVER_PORT = 5353;

    @Provides
    RpcServer provideRpcServer(NioReactor reactor, Config config) {
        return RpcServer.create(reactor)
                // You can define any message types by class
                .withMessageTypes(String.class)
                // Your message handlers can be written below
                .withHandler(String.class, request -> {
                    if (request.equalsIgnoreCase("hello") || request.equalsIgnoreCase("hi")) {
                        return Promise.of("Hi, user!");
                    }
                    if (request.equalsIgnoreCase("What is your name?")) {
                        return Promise.of("My name is ... RPC Server :)");
                    }
                    return Promise.of(request + " " + request);
                })
                .withListenPort(config.get(ConfigConverters.ofInteger(), "client.connectionPort", RPC_SERVER_PORT));
    }

    @Override
    protected void run() throws Exception {
        awaitShutdown();
    }

    public static void main(String[] args) throws Exception {
        MyRpcServer rpcApp = new MyRpcServer();
        rpcApp.launch(args);
    }
}
