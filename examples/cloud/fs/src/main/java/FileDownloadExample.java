import io.activej.csp.ChannelSupplier;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.eventloop.Eventloop;
import io.activej.fs.RemoteActiveFs;
import io.activej.inject.Injector;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * This example demonstrates downloading file from RemoteFS server.
 * To run this example you should first launch ServerSetupExample and then FileUploadExample
 */
@SuppressWarnings("unused")
public final class FileDownloadExample extends Launcher {
	private static final int SERVER_PORT = 6732;
	private static final String REQUIRED_FILE = "example.txt";
	private static final String DOWNLOADED_FILE = "downloaded_example.txt";

	private Path clientStorage;

	@Override
	protected void onInit(Injector injector) throws Exception {
		clientStorage = Files.createTempDirectory("client_storage");
	}

	@Inject
	private RemoteActiveFs client;

	@Inject
	private Eventloop eventloop;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	RemoteActiveFs remoteFsClient(Eventloop eventloop) {
		return RemoteActiveFs.create(eventloop, new InetSocketAddress(SERVER_PORT));
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	//[START EXAMPLE]
	@Override
	protected void run() throws Exception {
		ExecutorService executor = newSingleThreadExecutor();
		CompletableFuture<Void> future = eventloop.submit(() ->
				ChannelSupplier.ofPromise(client.download(REQUIRED_FILE))
						.streamTo(ChannelFileWriter.open(executor, clientStorage.resolve(DOWNLOADED_FILE)))
						.whenResult(() -> System.out.printf("%nFile '%s' successfully downloaded to '%s'%n%n",
								REQUIRED_FILE, clientStorage))
		);
		future.get();
		executor.shutdown();
	}
	//[END EXAMPLE]

	public static void main(String[] args) throws Exception {
		FileDownloadExample example = new FileDownloadExample();
		example.launch(args);
	}
}
