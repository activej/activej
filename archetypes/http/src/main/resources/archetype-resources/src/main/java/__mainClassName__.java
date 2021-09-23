package ${groupId};

import io.activej.http.AsyncServlet;
import io.activej.http.HttpResponse;
import io.activej.inject.annotation.Provides;
import io.activej.launcher.Launcher;
import io.activej.launchers.http.HttpServerLauncher;
import io.activej.promise.Promise;

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
