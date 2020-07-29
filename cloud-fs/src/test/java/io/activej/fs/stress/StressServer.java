package io.activej.fs.stress;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.activej.eventloop.Eventloop;
import io.activej.fs.LocalActiveFs;
import io.activej.fs.tcp.ActiveFsServer;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;
import static io.activej.fs.util.Utils.initTempDir;
import static io.activej.test.TestUtils.getFreePort;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class StressServer {

	static {
		((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.INFO);
	}

	static final Path STORAGE_PATH = Paths.get("./test_data/server_storage");
	private static final int PORT = getFreePort();

	private static final ExecutorService executor = newCachedThreadPool();
	private static final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

	public static final LocalActiveFs localFs = LocalActiveFs.create(eventloop, executor, STORAGE_PATH);

	public static final ActiveFsServer server = ActiveFsServer.create(eventloop, localFs)
			.withListenPort(PORT);

	public static void main(String[] args) throws IOException {
		initTempDir(STORAGE_PATH);
		server.listen();
		eventloop.run();
		executor.shutdown();
	}
}
