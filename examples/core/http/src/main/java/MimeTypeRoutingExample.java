import io.activej.http.*;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.reactor.Reactor;

/**
 * An example of setting routes based on Content-Type MIME types.
 * <p>
 * You may test server behaviour by issuing {@code curl} commands:
 * <ul>
 *     <li>{@code curl -X POST -H "Content-Type: image/png" http://localhost:8080}</li>
 *     <li>{@code curl -X POST -H "Content-Type: text/plain" http://localhost:8080}</li>
 * </ul>
 */
public final class MimeTypeRoutingExample extends HttpServerLauncher {

	private static final String IMAGE_TYPE_PREFIX = "image/";
	private static final String TEXT_TYPE_PREFIX = "text/";

	@Provides
	AsyncServlet mainServlet(Reactor reactor, @Named("Image") AsyncServlet imageServlet, @Named("Text") AsyncServlet textServlet) {
		return RoutingServlet.create(reactor)
				.map(HttpMethod.POST, "/*", request -> {
					String contentType = request.getHeader(HttpHeaders.CONTENT_TYPE);
					if (contentType == null) {
						return HttpResponse.builder(400)
								.withPlainText("'Content-Type' header is missing")
								.toPromise();
					}
					if (isImageType(contentType)) {
						return imageServlet.serve(request);
					} else if (isTextType(contentType)) {
						return textServlet.serve(request);
					} else {
						return HttpResponse.builder(400)
								.withPlainText("Unsupported mime type in 'Content-Type' header")
								.toPromise();
					}
				});
	}

	@Provides
	@Named("Image")
	AsyncServlet imageServlet() {
		return request -> HttpResponse.Builder.ok200()
				.withPlainText("This servlet handles images\n")
				.toPromise();
	}

	@Provides
	@Named("Text")
	AsyncServlet textServlet() {
		return request -> HttpResponse.Builder.ok200()
				.withPlainText("This servlet handles text data\n")
				.toPromise();
	}

	private static boolean isTextType(String mime) {
		return mime.startsWith(TEXT_TYPE_PREFIX);
	}

	private static boolean isImageType(String mime) {
		return mime.startsWith(IMAGE_TYPE_PREFIX);
	}

	public static void main(String[] args) throws Exception {
		new MimeTypeRoutingExample().launch(args);
	}
}
