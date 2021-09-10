import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.dns.AsyncDnsClient;
import io.activej.dns.RemoteAsyncDnsClient;
import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpClient;
import io.activej.http.HttpRequest;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.activej.config.converter.ConfigConverters.ofDuration;
import static io.activej.config.converter.ConfigConverters.ofInetAddress;
import static io.activej.inject.module.Modules.combine;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * HTTP client example.
 * You can launch HttpServerExample to test this.
 */
@SuppressWarnings("Convert2MethodRef")
public final class HttpClientExample extends Launcher {
	@Inject
	AsyncHttpClient httpClient;

	@Inject
	Eventloop eventloop;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	//[START REGION_1]
	@Provides
	AsyncHttpClient client(Eventloop eventloop, AsyncDnsClient dnsClient) {
		return AsyncHttpClient.create(eventloop)
				.withDnsClient(dnsClient);
	}

	@Provides
	AsyncDnsClient dnsClient(Eventloop eventloop, Config config) {
		return RemoteAsyncDnsClient.create(eventloop)
				.withDnsServerAddress(config.get(ofInetAddress(), "dns.address"))
				.withTimeout(config.get(ofDuration(), "dns.timeout"));
	}
	//[END REGION_1]

	//[START REGION_2]
	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger());
	}

	@Provides
	Config config() {
		return Config.create()
				.with("dns.address", "8.8.8.8")
				.with("dns.timeout", "5 seconds")
				.overrideWith(Config.ofSystemProperties("config"));
	}
	//[END REGION_2]

	//[START REGION_3]
	@Override
	protected void run() throws ExecutionException, InterruptedException {
		String url = args.length != 0 ? args[0] : "http://127.0.0.1:8080/";
		System.out.println("\nHTTP request: " + url);
		CompletableFuture<String> future = eventloop.submit(() ->
				httpClient.request(HttpRequest.get(url))
						.then(response -> response.loadBody())
						.map(body -> body.getString(UTF_8))
		);
		System.out.println("HTTP response: " + future.get());
		System.out.println();
	}
	//[END REGION_3]

	public static void main(String[] args) throws Exception {
		HttpClientExample example = new HttpClientExample();
		example.launch(args);
	}
}
