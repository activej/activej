package io.activej.remotefs.stress;

import io.activej.common.ref.RefInt;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.eventloop.Eventloop;
import io.activej.remotefs.RemoteFsClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;

public class StressDownload {
	private static final int OPERATIONS_QUANTITY = 10 * 1024;
	private static final int FILE_MAX_SIZE = 1024;

	private static final Path CLIENT_STORAGE = Paths.get("./test_data/client_storage");

	private static Random rand = new Random();
	public static final List<String> FILES = new ArrayList<>();

	public static void main(String[] args) throws IOException {

		Files.createDirectories(CLIENT_STORAGE);

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = Executors.newCachedThreadPool();

		RefInt failures = new RefInt(1);

		RemoteFsClient client = RemoteFsClient.create(eventloop, new InetSocketAddress("localhost", 5560));

		for (int i = 0; i < OPERATIONS_QUANTITY; i++) {
			FILES.add(createFile());
		}

		for (int i = 0; i < OPERATIONS_QUANTITY; i++) {
			String file = FILES.get(rand.nextInt(OPERATIONS_QUANTITY));
			client.download(file)
					.whenComplete((supplier, e) -> {
						if (e == null) {
							supplier.streamTo(ChannelFileWriter.open(executor, CLIENT_STORAGE.resolve(file)));
						} else {
							failures.value++;
						}
					});

			eventloop.run();
		}

		executor.shutdown();
		System.out.println("Failures: " + failures.value);
	}

	public static String createFile() throws IOException {
		String name = Integer.toString(rand.nextInt(Integer.MAX_VALUE));
		Path file = StressServer.STORAGE_PATH.resolve(name);
		byte[] bytes = new byte[FILE_MAX_SIZE];
		rand.nextBytes(bytes);
		Files.write(file, bytes);
		return name;
	}
}
