package io.activej;

import io.activej.dns.DnsClient;
import io.activej.eventloop.Eventloop;
import io.activej.http.*;

import java.io.IOException;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.common.exception.FatalErrorHandlers.rethrow;
import static io.activej.http.HttpHeaders.ACCEPT_ENCODING;
import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpUtils.inetAddress;
import static io.activej.test.TestUtils.getFreePort;

public final class GzipCompressingBehaviourExample {
	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.builder()
			.withFatalErrorHandler(rethrow())
			.withCurrentThread()
			.build();
		RoutingServlet servlet = RoutingServlet.builder(eventloop)
			// always responds in gzip
			.with(GET, "/gzip/",
				request -> HttpResponse.ok200()
					.withBodyGzipCompression()
					.withBody(encodeAscii("Hello!"))
					.toPromise())
			// never responds in gzip
			.with(GET, "/nogzip/",
				request -> HttpResponse.ok200()
					.withBody(encodeAscii("Hello!"))
					.toPromise())
			.build();

		HttpServer.builder(eventloop, servlet)
			.withListenPort(getFreePort())
			.build()
			.listen();

		eventloop.run();

		// this is how you should send an HTTP request with gzipped body.
		// if the content of the response is gzipped - it would be decompressed automatically
		DnsClient dnsClient = DnsClient.create(eventloop, inetAddress("8.8.8.8"));
		IHttpClient client = HttpClient.create(eventloop, dnsClient);

		// !sic, you should call withAcceptEncodingGzip for your request if you want to get the response gzipped
		client.request(
			HttpRequest.post("http://example.com")
				.withBody(encodeAscii("Hello, world!"))
				.withBodyGzipCompression()
				.withHeader(ACCEPT_ENCODING, "gzip")
				.build());
	}
}
