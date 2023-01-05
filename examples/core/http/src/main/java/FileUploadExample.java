import io.activej.csp.file.ChannelFileWriter;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.MultipartDecoder.AsyncMultipartDataHandler;
import io.activej.http.RoutingServlet;
import io.activej.http.StaticServlet;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;

import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpMethod.POST;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class FileUploadExample extends HttpServerLauncher {
	private Path path;

	@Override
	protected void onInit(Injector injector) throws Exception {
		path = Files.createTempDirectory("upload-example");
	}

	@Provides
	Executor executor() {
		return newSingleThreadExecutor();
	}

	//[START EXAMPLE]
	@Provides
	AsyncServlet servlet(Executor executor) {
		return RoutingServlet.create()
				.map(GET, "/*", StaticServlet.ofClassPath(executor, "static/multipart/")
						.withIndexHtml())
				.map(POST, "/test", request ->
						request.handleMultipart(AsyncMultipartDataHandler.file(fileName -> ChannelFileWriter.open(executor, path.resolve(fileName))))
								.map($ -> HttpResponse.ok200().withPlainText("Upload successful")));
	}
	//[END EXAMPLE]

	public static void main(String[] args) throws Exception {
		Launcher launcher = new FileUploadExample();
		launcher.launch(args);
	}
}
