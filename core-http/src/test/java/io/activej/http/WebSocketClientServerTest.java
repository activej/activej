package io.activej.http;

import io.activej.common.ref.RefInt;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.http.WebSocket.Message;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.activej.http.WebSocketConstants.HANDSHAKE_FAILED;
import static io.activej.https.SslUtils.createTestSslContext;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class WebSocketClientServerTest {
	private static final int PORT = getFreePort();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Test
	public void testEcho() throws IOException {
		startTestServer(ws -> ws.messageReadChannel().streamTo(ws.messageWriteChannel()));

		Stream<String> inputStream = IntStream.range(0, 100).mapToObj(String::valueOf);

		String result = await(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.webSocketRequest(HttpRequest.webSocket("ws://127.0.0.1:" + PORT))
				.then(ws -> {
					ChannelSupplier.ofStream(inputStream)
							.mapAsync(item -> Promises.delay(1L, Message.text(item)))
							.streamTo(ws.messageWriteChannel());
					return ws.messageReadChannel()
							.map(Message::getText)
							.toCollector(toList())
							.map(strings -> String.join("", strings));
				}));

		assertEquals(IntStream.range(0, 100).mapToObj(String::valueOf).collect(joining()), result);
	}

	@Test
	public void testServerWSException() throws IOException {
		RefInt counter = new RefInt(100);
		List<Message> messages = new ArrayList<>();
		String reason = "Some error";
		WebSocketException exception = new WebSocketException(getClass(), 4321, reason);

		startTestServer(webSocket -> ChannelSupplier.of(() -> Promise.of("hello"))
				.mapAsync(
						buf -> {
							if (counter.dec() < 0) {
								return Promise.ofException(exception);
							} else {
								return Promise.of(Message.text(buf));
							}
						})
				.streamTo(webSocket.messageWriteChannel()));

		Throwable receivedEx = awaitException(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.webSocketRequest(HttpRequest.webSocket("ws://127.0.0.1:" + PORT))
				.then(webSocket -> webSocket.messageReadChannel().streamTo(ChannelConsumer.ofConsumer(messages::add))));

		assertThat(receivedEx, instanceOf(WebSocketException.class));
		assertEquals(Integer.valueOf(4321), ((WebSocketException) receivedEx).getCode());
		assertEquals(reason, ((WebSocketException) receivedEx).getReason());

		assertEquals(100, messages.size());
		for (Message msg : messages) {
			assertEquals("hello", msg.getText());
		}
	}

	@Test
	public void testSecureWebSocketsCloseByClient() throws IOException {
		WebSocketException testError = new WebSocketException(getClass(), 4321, "Test error");
		ExecutorService executor = Executors.newSingleThreadExecutor();
		SettablePromise<WebSocketException> settablePromise = new SettablePromise<>();

		startSecureTestServer(webSocket -> webSocket.readFrame()
				.whenException(settablePromise::setException));

		await(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.withSslEnabled(createTestSslContext(), executor)
				.webSocketRequest(HttpRequest.webSocket("wss://127.0.0.1:" + PORT))
				.whenResult(webSocket -> webSocket.closeEx(testError)));

		WebSocketException exception = awaitException(settablePromise);

		assertEquals(testError.getCode(), exception.getCode());
		assertEquals(testError.getMessage(), exception.getMessage());
		executor.shutdown();
	}

	@Test
	public void testSecureWebSocketsCloseByServer() throws IOException {
		WebSocketException testError = new WebSocketException(getClass(), 4321, "Test error");
		ExecutorService executor = Executors.newSingleThreadExecutor();

		startSecureTestServer(webSocket -> webSocket.closeEx(testError));

		WebSocketException exception = awaitException(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.withSslEnabled(createTestSslContext(), executor)
				.webSocketRequest(HttpRequest.webSocket("wss://127.0.0.1:" + PORT))
				.then(webSocket -> webSocket.messageReadChannel()
						.streamTo(ChannelConsumer.ofConsumer($ -> fail()))));

		assertEquals(testError.getCode(), exception.getCode());
		assertEquals(testError.getMessage(), exception.getMessage());
		executor.shutdown();
	}

	@Test
	public void testRejectedHandshake() throws IOException {
		AsyncHttpServer.create(Eventloop.getCurrentEventloop(), RoutingServlet.create()
				.mapWebSocket("/", ($, fn) -> fn.apply(HttpResponse.ofCode(400))))
				.withListenPort(PORT)
				.withAcceptOnce()
				.listen();
		Throwable exception = awaitException(AsyncHttpClient.create(Eventloop.getCurrentEventloop())
				.webSocketRequest(HttpRequest.webSocket("ws://127.0.0.1:" + PORT)));

		assertEquals(HANDSHAKE_FAILED, exception);
	}

	private void startTestServer(Consumer<WebSocket> webSocketConsumer) throws IOException {
		AsyncHttpServer.create(Eventloop.getCurrentEventloop(), RoutingServlet.create()
				.mapWebSocket("/", webSocketConsumer))
				.withListenPort(PORT)
				.withAcceptOnce()
				.listen();
	}

	private void startSecureTestServer(Consumer<WebSocket> webSocketConsumer) throws IOException {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		AsyncHttpServer server = AsyncHttpServer.create(Eventloop.getCurrentEventloop(), RoutingServlet.create()
				.mapWebSocket("/", webSocketConsumer))
				.withSslListenPort(createTestSslContext(), executor, PORT)
				.withAcceptOnce();
		server.getCloseNotification().whenComplete(executor::shutdown);
		server.listen();
	}

}
