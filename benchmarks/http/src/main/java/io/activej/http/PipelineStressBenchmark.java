package io.activej.http;

import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import static java.lang.String.join;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;

public final class PipelineStressBenchmark extends Launcher {
	public static final String REQUEST = """
			GET /plaintext HTTP/1.1\r
			Host: activej.io\r
			Accept: text/plain,text/html;q=0.9,application/xhtml+xml;q=0.9,application/xml;q=0.8,*/*;q=0.7\r
			Connection: keep-alive\r
			\r
			""";

	public static final int TOTAL_REQUESTS = 5_000_000;
	public static final int PIPELINE_COUNT = 16;

	public static final int WARMUP_ROUNDS = 3;
	public static final int MEASUREMENT_ROUNDS = 5;

	public static final byte[] REQUEST_BYTES = join("", nCopies(PIPELINE_COUNT, REQUEST)).getBytes();

	public static final int BUFFER = 16384;
	public static final int PORT = 8080;

	@Inject
	HttpServer server;

	@Provides
	NioReactor eventloop() {
		return Eventloop.create();
	}

	@Provides
	HttpServer server(NioReactor reactor) {
		return HttpServer.builder(reactor, request -> HttpResponse.Builder.ok200()
						.withPlainText("Hello, world!")
						.toPromise())
				.withListenPort(PORT)
				.build();
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void run() throws Exception {
		for (int i = 0; i < WARMUP_ROUNDS; i++) {
			long elapsed = round();
			System.out.println("Warmup, RPS: " + TOTAL_REQUESTS * 1000L / elapsed);
		}

		int sum = 0;
		for (int i = 0; i < MEASUREMENT_ROUNDS; i++) {
			long elapsed = round();
			long rps = TOTAL_REQUESTS * 1000L / elapsed;
			System.out.println("Measurement, RPS: " + rps);
			sum += rps;
		}

		System.out.println("Average RPS: " + sum / MEASUREMENT_ROUNDS);

		server.closeFuture().get();
	}

	private long round() throws IOException, InterruptedException {
		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress("localhost", PORT));

			Thread writeThread = writeThread(socket);
			Thread readThread = readThread(socket);

			long before = System.currentTimeMillis();

			writeThread.start();
			readThread.start();

			readThread.join();
			writeThread.join();

			return System.currentTimeMillis() - before;
		}
	}

	private Thread readThread(Socket socket) {
		return new Thread(() -> {
			int read = 0;
			byte[] bytes = new byte[BUFFER];
			try {
				InputStream is = socket.getInputStream();
				do {
					read = is.read(bytes);
				} while (read != -1);
				socket.shutdownInput();
			} catch (IOException e) {
				throw new RuntimeException("Could not read " + new String(bytes, 0, read, UTF_8), e);
			}
		});
	}

	private Thread writeThread(Socket socket) {
		return new Thread(() -> {
			int limit = TOTAL_REQUESTS / PIPELINE_COUNT;
			try {
				OutputStream outputStream = socket.getOutputStream();
				for (int i = 0; i < limit; i++) {
					outputStream.write(REQUEST_BYTES, 0, REQUEST_BYTES.length);
				}
				socket.shutdownOutput();
			} catch (IOException e) {
				throw new RuntimeException("Could not write", e);
			}
		});
	}

	public static void main(String[] args) throws Exception {
		new PipelineStressBenchmark().launch(args);
	}
}
