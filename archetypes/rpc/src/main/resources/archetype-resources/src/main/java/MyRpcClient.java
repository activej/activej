package ${groupId};

import io.activej.codegen.DefiningClassLoader;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.RpcStrategies;
import io.activej.serializer.SerializerBuilder;
import io.activej.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.util.Scanner;

import static io.activej.config.converter.ConfigConverters.ofInteger;

@SuppressWarnings({"WeakerAccess", "unused"})
public class MyRpcClient extends Launcher {
    private static final int RPC_LISTENER_PORT = 5353;

    @Inject
    Eventloop eventloop;

    @Inject
    RpcClient client;

    @Provides
    Config config() {
        return Config.ofProperties(System.getProperties()).getChild("config");
    }

    @Provides
    Eventloop eventloop() {
        return Eventloop.create();
    }

    @Provides
    RpcClient rpcClient(Eventloop eventloop, Config config) {
        return RpcClient.create(eventloop)
                .withSerializerBuilder(SerializerBuilder.create(DefiningClassLoader.create(Thread.currentThread().getContextClassLoader())))
                .withMessageTypes(String.class)
                .withStrategy(RpcStrategies.server(
                        new InetSocketAddress(config.get(ofInteger(), "port", RPC_LISTENER_PORT))));
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
            eventloop.submit(() -> client.sendRequest(line))
                    .thenAccept(string -> System.out.println("Response: " + string + "\n"))
                    .get();
        }
    }

    public static void main(String[] args) throws Exception {
        MyRpcClient client = new MyRpcClient();
        client.launch(args);
    }
}
