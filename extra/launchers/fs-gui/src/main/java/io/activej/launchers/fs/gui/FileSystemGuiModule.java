package io.activej.launchers.fs.gui;

import io.activej.config.Config;
import io.activej.fs.IFileSystem;
import io.activej.http.AsyncServlet;
import io.activej.http.HttpServer;
import io.activej.inject.Key;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.Multibinders;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;

import static io.activej.launchers.initializers.Initializers.ofHttpServer;

public final class FileSystemGuiModule extends AbstractModule {
	public static final String DEFAULT_GUI_SERVER_LISTEN_ADDRESS = "*:8080";

	private FileSystemGuiModule() {
	}

	public static FileSystemGuiModule create() {
		return new FileSystemGuiModule();
	}

	@Override
	protected void configure() {
		multibind(Key.of(Config.class), Multibinders.ofBinaryOperator(Config::combineWith));
	}

	@Provides
	@Eager
	HttpServer guiServer(NioReactor reactor, AsyncServlet servlet, Config config) {
		return HttpServer.builder(reactor, servlet)
			.initialize(ofHttpServer(config.getChild("fs.http.gui")))
			.build();
	}

	@Provides
	AsyncServlet guiServlet(Reactor reactor, IFileSystem fileSystem) {
		return FileSystemGuiServlet.create(reactor, fileSystem);
	}

	@Provides
	Config config() {
		return Config.create()
			.with("fs.http.gui.listenAddresses", DEFAULT_GUI_SERVER_LISTEN_ADDRESS);
	}
}
