package io.activej.https;

import io.activej.dns.AsyncDnsClient;
import io.activej.dns.DnsClient_Cached;
import io.activej.dns.DnsClient_Reactive;
import io.activej.http.*;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.concurrent.Executors;

import static io.activej.http.HttpHeaderValue.ofAcceptMediaTypes;
import static io.activej.http.HttpHeaders.*;
import static io.activej.http.HttpUtils.inetAddress;
import static io.activej.http.MediaTypes.*;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public final class TestHttpsClient {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	@Ignore("Connects to external URL, may fail on no internet connection")
	public void testClient() throws NoSuchAlgorithmException {
		NioReactor reactor = Reactor.getCurrentReactor();

		AsyncDnsClient dnsClient = DnsClient_Cached.create(reactor, DnsClient_Reactive.create(reactor)
				.withTimeout(Duration.ofMillis(500))
				.withDnsServerAddress(inetAddress("8.8.8.8")));

		AsyncHttpClient client = HttpClient_Reactive.create(reactor)
				.withDnsClient(dnsClient)
				.withSslEnabled(SSLContext.getDefault(), Executors.newSingleThreadExecutor());
		Integer code = await(client.request(HttpRequest.get("https://en.wikipedia.org/wiki/Wikipedia")
						.withHeader(CACHE_CONTROL, "max-age=0")
						.withHeader(ACCEPT_ENCODING, "gzip, deflate, sdch")
						.withHeader(ACCEPT_LANGUAGE, "en-US,en;q=0.8")
						.withHeader(USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36")
						.withHeader(ACCEPT, ofAcceptMediaTypes(
								AcceptMediaType.of(HTML),
								AcceptMediaType.of(XHTML_APP),
								AcceptMediaType.of(XML_APP, 90),
								AcceptMediaType.of(WEBP),
								AcceptMediaType.of(ANY, 80))))
				.map(HttpResponse::getCode));

		assertEquals((Integer) 200, code);
	}
}
