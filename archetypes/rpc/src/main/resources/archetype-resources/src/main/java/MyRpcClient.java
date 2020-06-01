package ${groupId};

import io.activej.config.Config;
import io.activej.di.annotation.Inject;
import io.activej.di.annotation.Provides;
import io.activej.di.module.Module;
import io.activej.eventloop.Eventloop;
import io.activej.launcher.Launcher;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.RpcStrategies;
import io.activej.serializer.SerializerBuilder;
import io.activej.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.util.Scanner;

import static io.activej.config.ConfigConverters.ofInteger;

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
                .withSerializerBuilder(SerializerBuilder.create(ClassLoader.getSystemClassLoader()))
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
            if (line.toLowerCase().equals("exit")) {
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
