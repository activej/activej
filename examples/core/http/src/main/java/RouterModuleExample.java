import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.http.inject.RouterModule;
import io.activej.http.inject.RouterModule.Mapped;
import io.activej.http.inject.RouterModule.Router;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launchers.http.HttpServerLauncher;

//[START EXAMPLE]
public final class RouterModuleExample extends HttpServerLauncher {

	@Override
	protected Module getBusinessLogicModule() {
		return Modules.combine(new RouterModule());
	}

	@Provides
	AsyncServlet servlet(@Router AsyncServlet router) {
		return router;
	}

	@Provides
	@Mapped("/")
	AsyncServlet main() {
		return request -> HttpResponse.ok200().withPlainText("hello world");
	}

	@Provides
	@Mapped("/test1")
	AsyncServlet test1() {
		return request -> HttpResponse.ok200().withPlainText("this is test 1");
	}

	@Provides
	@Mapped("/*")
	AsyncServlet others() {
		return request -> HttpResponse.ok200().withPlainText("this is the fallback: " + request.getRelativePath());
	}

	public static void main(String[] args) throws Exception {
		Injector.useSpecializer();

		RouterModuleExample example = new RouterModuleExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
