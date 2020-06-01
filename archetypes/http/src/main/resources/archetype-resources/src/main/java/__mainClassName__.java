package ${groupId};

import io.activej.promise.Promise;
import io.activej.di.annotation.Provides;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;

public class ${mainClassName} extends HttpServerLauncher {

    @Provides
    AsyncServlet servlet() {
        return request -> Promise.of(
                HttpResponse.ok200()
                    .withPlainText("Hello, World!"));
    }

    public static void main(String[] args) throws Exception {
        Launcher launcher = new ${mainClassName}();
        launcher.launch(args);
    }
}
