package io.activej.http;

import io.activej.eventloop.Eventloop;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.common.Utils.first;
import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HelloWorldPostServer {
	public static final String HELLO_WORLD = "Hello, World!";

	public static AsyncHttpServer helloWorldServer(Eventloop primaryEventloop) {
		return AsyncHttpServer.create(primaryEventloop,
				request -> request.loadBody()
						.map(body -> HttpResponse.ok200()
								.withBody(encodeAscii(HELLO_WORLD + body.getString(UTF_8)))))
				.withListenPort(0);
	}

	public static void main(String[] args) throws Exception {
		Eventloop primaryEventloop = Eventloop.create().withEventloopFatalErrorHandler(rethrow()).withCurrentThread();

		AsyncHttpServer httpServerListener = helloWorldServer(primaryEventloop);

		System.out.println("Start HelloWorld HTTP Server on :" + first(httpServerListener.getBoundAddresses()).getPort());
		httpServerListener.listen();

		primaryEventloop.run();
	}

}
