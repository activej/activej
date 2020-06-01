import io.activej.di.annotation.Provides;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;

//[START EXAMPLE]
public final class HttpHelloWorldExample extends HttpServerLauncher {
	@Provides
	AsyncServlet servlet() {
		return request -> HttpResponse.ok200().withPlainText("Hello World");
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpHelloWorldExample();
		launcher.launch(args);
	}
}
//[END EXAMPLE]
