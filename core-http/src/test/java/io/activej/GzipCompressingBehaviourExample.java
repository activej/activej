package io.activej;

import io.activej.eventloop.Eventloop;
import io.activej.http.*;

import java.io.IOException;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.common.exception.FatalErrorHandlers.rethrow;
import static io.activej.http.HttpHeaders.ACCEPT_ENCODING;
import static io.activej.http.HttpMethod.GET;
import static io.activej.test.TestUtils.getFreePort;

public final class GzipCompressingBehaviourExample {
	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.builder()
				.withFatalErrorHandler(rethrow())
				.withCurrentThread()
				.build();
		RoutingServlet servlet = RoutingServlet.create(eventloop)
				// always responds in gzip
				.map(GET, "/gzip/",
						request -> HttpResponse.Builder.ok200()
								.withBodyGzipCompression()
								.withBody(encodeAscii("Hello!"))
								.build())
				// never responds in gzip
				.map(GET, "/nogzip/",
						request -> HttpResponse.Builder.ok200()
								.withBody(encodeAscii("Hello!"))
								.build());

		HttpServer.builder(eventloop, servlet)
				.withListenPort(getFreePort())
				.build()
				.listen();

		eventloop.run();

		// this is how you should send an http request with gzipped body.
		// if the content of the response is gzipped - it would be decompressed automatically
		IHttpClient client = HttpClient.create(eventloop);

		// !sic, you should call withAcceptEncodingGzip for your request if you want to get the response gzipped
		client.request(
				HttpRequest.Builder.post("http://example.com")
						.withBody(encodeAscii("Hello, world!"))
						.withBodyGzipCompression()
						.withHeader(ACCEPT_ENCODING, "gzip")
						.build());
	}
}
