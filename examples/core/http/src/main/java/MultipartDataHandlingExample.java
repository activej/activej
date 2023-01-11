import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.http.*;
import io.activej.http.ByteBufsDecoder_Multipart.AsyncMultipartDataHandler;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.http.HttpMethod.POST;

public final class MultipartDataHandlingExample extends HttpServerLauncher {
	private static final String CRLF = "\r\n";
	private static final String BOUNDARY = "-----------------------------4336597275426690519140415448";
	private static final String MULTIPART_REQUEST = BOUNDARY + CRLF +
			"Content-Disposition: form-data; name=\"id\"" + CRLF +
			CRLF +
			"12345" + CRLF +
			BOUNDARY + CRLF +
			"Content-Disposition: form-data; name=\"file1\"; filename=\"data.txt\"" + CRLF +
			"Content-Type: text/plain" + CRLF +
			CRLF +
			"Contet of data.txt" + CRLF +
			BOUNDARY + CRLF +
			"Content-Disposition: form-data; name=\"first name\"" + CRLF +
			CRLF +
			"Alice" + CRLF +
			BOUNDARY + CRLF +
			"Content-Disposition: form-data; name=\"file2\"; filename=\"key.txt\"" + CRLF +
			"Content-Type: text/html" + CRLF +
			CRLF +
			"Content of key.txt" + CRLF +
			BOUNDARY + CRLF +
			"Content-Disposition: form-data; name=\"last name\"" + CRLF +
			CRLF +
			"Johnson" + CRLF +
			BOUNDARY + "--" + CRLF;

	private Path path;
	private int fileUploadsCount;

	@Override
	protected void onInit(Injector injector) throws Exception {
		path = Files.createTempDirectory("multipart-data-files");
	}

	@Inject
	Reactor reactor;

	@Inject
	Executor executor;

	@Inject
	AsyncHttpClient client;

	@Provides
	AsyncHttpClient client(NioReactor reactor) {
		return HttpClient_Reactive.create(reactor);
	}

	@Provides
	Executor executor() {
		return Executors.newSingleThreadExecutor();
	}

	//[START SERVLET]
	@Provides
	AsyncServlet servlet() {
		return Servlet_Routing.create()
				.map(POST, "/handleMultipart", request -> {
					Map<String, String> fields = new HashMap<>();

					return request.handleMultipart(AsyncMultipartDataHandler.fieldsToMap(fields, this::upload))
							.map($ -> {
								logger.info("Received fields: {}", fields);
								logger.info("Uploaded {} files total", fileUploadsCount);
								return HttpResponse.ok200();
							});
				});
	}
	//[END SERVLET]

	@Override
	protected void run() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> future = reactor.submit(() ->
				client.request(HttpRequest.post("http://localhost:8080/handleMultipart")
								.withHeader(HttpHeaders.CONTENT_TYPE, "multipart/form-data; boundary=" + BOUNDARY.substring(2))
								.withBody(ByteBufStrings.encodeAscii(MULTIPART_REQUEST)))
						.map(HttpResponse::getCode));

		int code = future.get();

		if (code != 200) {
			throw new RuntimeException("Did not receive OK response: " + code);
		}
	}

	//[START UPLOAD]
	private Promise<ChannelConsumer<ByteBuf>> upload(String filename) {
		logger.info("Uploading file '{}' to {}", filename, path);
		return ChannelFileWriter.open(executor, path.resolve(filename))
				.map(writer -> writer.withAcknowledgement(ack ->
						ack.whenResult(() -> {
							logger.info("Upload of file '{}' finished", filename);
							fileUploadsCount++;
						})));
	}
	//[END UPLOAD]

	public static void main(String[] args) throws Exception {
		Launcher launcher = new MultipartDataHandlingExample();
		launcher.launch(args);
	}
}
