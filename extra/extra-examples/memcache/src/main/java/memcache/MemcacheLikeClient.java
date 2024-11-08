package memcache;

import io.activej.codegen.DefiningClassLoader;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.launcher.Launcher;
import io.activej.memcache.client.MemcacheClientModule;
import io.activej.memcache.client.RawMemcacheClient;
import io.activej.promise.Promises;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.activej.promise.Promises.sequence;
import static java.util.stream.IntStream.range;

//[START REGION_1]
public class MemcacheLikeClient extends Launcher {

	@Inject
	RawMemcacheClientAdapter client;

	@Inject
	NioReactor reactor;

	@Provides
	NioReactor reactor() {
		return Eventloop.create();
	}

	@Provides
	RawMemcacheClientAdapter rawMemcacheClientAdapter(RawMemcacheClient client) {
		return new RawMemcacheClientAdapter(client);
	}

	@Provides
	Config config() {
		return Config.create()
			.with("protocol.compression", "false")
			.with("client.addresses", "localhost:9000, localhost:9001, localhost:9002");
	}

	@Provides
	DefiningClassLoader classLoader() {
		return DefiningClassLoader.create();
	}

	@Override
	protected Module getModule() {
		return ModuleBuilder.create()
			.install(ServiceGraphModule.create())
			.install(MemcacheClientModule.create())
			.install(ConfigModule.builder()
				.withEffectiveConfigLogger()
				.build())
			.build();
	}

	@Override
	protected void run() throws ExecutionException, InterruptedException {
		String message = "Hello, Memcached Server";

		System.out.println();
		CompletableFuture<Void> future = reactor.submit(() ->
			sequence(
				() -> Promises.all(range(0, 25).mapToObj(i ->
					client.put(i, message))),
				() -> Promises.all(range(0, 25).mapToObj(i ->
					client.get(i).whenResult(res -> System.out.println(i + " : " + res))))));
		future.get();
		System.out.println();
	}

	public static void main(String[] args) throws Exception {
		Launcher client = new MemcacheLikeClient();
		client.launch(args);
	}
}
//[END REGION_1]
