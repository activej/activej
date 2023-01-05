package io.activej;

import io.activej.eventloop.Eventloop;
import io.activej.http.*;

import java.io.IOException;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.http.HttpHeaders.ACCEPT_ENCODING;
import static io.activej.http.HttpMethod.GET;
import static io.activej.test.TestUtils.getFreePort;

public final class GzipCompressingBehaviourExample {
	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrow()).withCurrentThread();
		RoutingServlet servlet = RoutingServlet.create()
				// always responds in gzip
				.map(GET, "/gzip/",
						request -> HttpResponse.ok200().withBodyGzipCompression().withBody(encodeAscii("Hello!")))
				// never responds in gzip
				.map(GET, "/nogzip/",
						request -> HttpResponse.ok200().withBody(encodeAscii("Hello!")));

		HttpServer server = HttpServer.create(eventloop, servlet).withListenPort(getFreePort());

		server.listen();
		eventloop.run();

		// this is how you should send an http request with gzipped body.
		// if the content of the response is gzipped - it would be decompressed automatically
		AsyncHttpClient client = ReactiveHttpClient.create(eventloop);

		// !sic, you should call withAcceptEncodingGzip for your request if you want to get the response gzipped
		client.request(
				HttpRequest.post("http://example.com")
						.withBody(encodeAscii("Hello, world!"))
						.withBodyGzipCompression()
						.withHeader(ACCEPT_ENCODING, "gzip"));
	}
}
