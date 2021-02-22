package io.activej.http;

import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ExecutionException;

import static java.lang.String.join;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;

public final class PipelineStressBenchmark extends Launcher {
	public static final String REQUEST = "GET /plaintext HTTP/1.1\r\n" +
			"Host: activej.io\r\n" +
			"Accept: text/plain,text/html;q=0.9,application/xhtml+xml;q=0.9,application/xml;q=0.8,*/*;q=0.7\r\n" +
			"Connection: keep-alive\r\n\r\n";

	public static final int TOTAL_REQUESTS = 50_000_000;
	public static final int PIPELINE_COUNT = 16;

	public static final byte[] REQUEST_BYTES = join("", nCopies(PIPELINE_COUNT, REQUEST)).getBytes();

	public static final int BUFFER = 16384;
	public static final int PORT = 8080;

	@Inject
	AsyncHttpServer server;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	AsyncHttpServer server(Eventloop eventloop) {
		return AsyncHttpServer.create(eventloop, request -> HttpResponse.ok200().withPlainText("Hello, world!"))
				.withListenPort(PORT);
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create()
				.combineWith(JmxModule.create());
	}

	@Override
	protected void run() throws Exception {
		Socket socket = new Socket();
		socket.connect(new InetSocketAddress("localhost", PORT));

		Thread writeThread = writeThread(socket);
		Thread readThread = readThread(socket);

		long before = System.currentTimeMillis();

		writeThread.start();
		readThread.start();

		readThread.join();
		writeThread.join();

		long elapsed = System.currentTimeMillis() - before;
		System.out.println("RPS: " + TOTAL_REQUESTS * 1000L / elapsed);
	}

	private Thread readThread(Socket socket) {
		return new Thread(() -> {
			byte[] bytes = new byte[BUFFER];
			try {
				InputStream is = socket.getInputStream();
				while (true) {
					int read = is.read(bytes);
					if (read == -1) break;
				}
			} catch (IOException e) {
				throw new RuntimeException("Could not read " + new String(bytes, UTF_8), e);
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
				server.closeFuture().get();
			} catch (IOException | InterruptedException | ExecutionException e) {
				throw new RuntimeException("Could not write", e);
			}
		});
	}

	public static void main(String[] args) throws Exception {
		new PipelineStressBenchmark().launch(args);
	}
}
