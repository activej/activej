package io.activej.https;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.activej.eventloop.Eventloop;
import io.activej.http.AsyncHttpServer;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.io.File;
import java.security.SecureRandom;
import java.util.concurrent.Executor;

import static io.activej.bytebuf.ByteBufStrings.wrapAscii;
import static io.activej.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.activej.https.SslUtils.*;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestHttpsServer {
	private static final int PORT = getFreePort();

	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.TRACE);
//		System.setProperty("javax.net.debug", "all");
	}

	public static void main(String[] args) throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		Executor executor = newCachedThreadPool();

		AsyncServlet bobServlet = request -> HttpResponse.ok200().withBody(wrapAscii("Hello, I am Bob!"));

		KeyManager[] keyManagers = createKeyManagers(new File("./src/test/resources/keystore.jks"), "testtest", "testtest");
		TrustManager[] trustManagers = createTrustManagers(new File("./src/test/resources/truststore.jks"), "testtest");

		AsyncHttpServer server = AsyncHttpServer.create(eventloop, bobServlet)
				.withSslListenPort(createSslContext("TLSv1", keyManagers, trustManagers, new SecureRandom()), executor, PORT)
				.withListenPort(getFreePort());

		System.out.println("https://127.0.0.1:" + PORT);

		server.listen();
		eventloop.run();
	}
}
