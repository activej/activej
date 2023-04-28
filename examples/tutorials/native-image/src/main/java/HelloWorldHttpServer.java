import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.inject.annotation.Provides;
import io.activej.launchers.http.HttpServerLauncher;

//[START EXAMPLE]
public class HelloWorldHttpServer extends HttpServerLauncher {

	@Provides
	AsyncServlet servlet() {
		return request -> HttpResponse.Builder.ok200()
				.withPlainText("Hello, world!")
				.build();
	}

	public static void main(String[] args) throws Exception {
		new HelloWorldHttpServer().launch(args);
	}
}
//[END EXAMPLE]

