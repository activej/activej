package $

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
