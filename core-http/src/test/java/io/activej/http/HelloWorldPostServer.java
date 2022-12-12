package io.activej.http;

import io.activej.eventloop.Eventloop;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HelloWorldPostServer {
	public static final int PORT = getFreePort();
	public static final String HELLO_WORLD = "Hello, World!";

	public static AsyncHttpServer helloWorldServer(Eventloop primaryEventloop, int port) {
		return AsyncHttpServer.create(primaryEventloop,
						request -> request.loadBody()
								.map(body -> HttpResponse.ok200()
										.withBody(encodeAscii(HELLO_WORLD + body.getString(UTF_8)))))
				.withListenPort(port);
	}

	public static void main(String[] args) throws Exception {
		Eventloop primaryEventloop = Eventloop.create().withFatalErrorHandler(rethrow()).withCurrentThread();

		AsyncHttpServer httpServerListener = helloWorldServer(primaryEventloop, PORT);

		System.out.println("Start HelloWorld HTTP Server on :" + PORT);
		httpServerListener.listen();

		primaryEventloop.run();
	}

}
