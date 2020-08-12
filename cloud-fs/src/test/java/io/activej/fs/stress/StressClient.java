package io.activej.fs.stress;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.MemSize;
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.fs.tcp.RemoteActiveFs;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import io.activej.serializer.annotations.Serialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.activej.eventloop.error.FatalErrorHandlers.rethrowOnAnyError;

class StressClient {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final InetSocketAddress address = new InetSocketAddress("localhost", 5560);
	private final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	private final ExecutorService executor = Executors.newCachedThreadPool();

	private final RemoteActiveFs client = RemoteActiveFs.create(eventloop, address);

	private static final Random rand = new Random();

	private static final Path clientStorage = Paths.get("./test_data/clients_storage");

	private static Path downloads;

	private final List<String> existingClientFiles = new ArrayList<>();
	private final List<Operation> operations = new ArrayList<>();

	public void setup() throws IOException {
		downloads = clientStorage.resolve("downloads");
		Files.createDirectories(downloads);

		// create and upload
		operations.add(() -> {
			try {
				String fileName = createFile();
				existingClientFiles.add(fileName);

				Path file = clientStorage.resolve(fileName);

				ChannelFileReader.open(executor, file)
						.map(cfr -> cfr.withBufferSize(MemSize.kilobytes(16)))
						.then(cfr -> cfr.streamTo(client.upload(fileName)))
						.whenComplete(($, e) -> {
							if (e == null) {
								logger.info("Uploaded: " + fileName);
							} else {
								logger.info("Failed to upload: {}", e.getMessage());
							}
						});
			} catch (IOException e) {
				logger.info(e.getMessage());
			}
		});

		// download
		operations.add(() -> {
			if (existingClientFiles.isEmpty()) return;

			int index = rand.nextInt(existingClientFiles.size());
			String fileName = existingClientFiles.get(index);

			if (fileName == null) return;

			client.download(fileName)
					.then(supplier -> supplier.streamTo(ChannelFileWriter.open(executor, downloads.resolve(fileName))))
					.whenComplete((supplier, e) -> {
						if (e == null) {
							logger.info("Downloaded: " + fileName);
						} else {
							logger.info("Failed to download: {}", e.getMessage());
						}
					});
		});

		// delete file
		operations.add(() -> {
			if (existingClientFiles.isEmpty()) return;

			int index = rand.nextInt(existingClientFiles.size());
			String fileName = existingClientFiles.get(index);

			client.delete(fileName).whenComplete(($, e) -> {
				if (e == null) {
					existingClientFiles.remove(fileName);
					logger.info("Deleted: " + fileName);
				} else {
					logger.info("Failed to delete: {}", e.getMessage());
				}
			});
		});

		// list file
		operations.add(() -> client.list("**").whenComplete((strings, e) -> {
			if (e == null) {
				logger.info("Listed: " + strings.size());
			} else {
				logger.info("Failed to list files: {}", e.getMessage());
			}
		}));

	}

	void start() throws IOException {
		setup();
		for (int i = 0; i < 100; i++) {
			eventloop.delay(rand.nextInt(360000), () ->
					operations.get(rand.nextInt(4)).go());
		}
		eventloop.run();
		executor.shutdown();
	}

	@FunctionalInterface
	private interface Operation {
		void go();
	}

	private String createFile() throws IOException {
		StringBuilder name = new StringBuilder();
		int nameLength = 5 + rand.nextInt(20);
		for (int i = 0; i < nameLength; i++) {
			name.append((char) (48 + rand.nextInt(74)));
		}

		Path file = clientStorage.resolve(name.toString());

		StringBuilder text = new StringBuilder();
		int textLength = rand.nextInt(1_000_000);
		for (int i = 0; i < textLength; i++) {
			text.append((char) (35 + rand.nextInt(60)));
			if (rand.nextBoolean())
				text.append("\r\n");
		}

		Files.write(file, text.toString().getBytes(StandardCharsets.UTF_8));

		return name.toString();
	}

	void uploadSerializedObject() throws UnknownHostException {
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		BinarySerializer<TestObject> binarySerializer = SerializerBuilder
				.create(classLoader)
				.build(TestObject.class);

		TestObject obj = new TestObject();
		obj.name = "someName";
		obj.ip = InetAddress.getLocalHost();

		StreamSupplier<TestObject> supplier = StreamSupplier.ofIterable(Collections.singletonList(obj));
		ChannelSerializer<TestObject> serializer = ChannelSerializer.create(binarySerializer)
				.withInitialBufferSize(ChannelSerializer.MAX_SIZE);

		//		supplier.with(serializer).streamTo(
		//				client.uploadStream("someName" + i));
		//		eventloop.run();
		throw new UnsupportedOperationException("TODO");
	}

	void downloadSmallObjects(int i) {
		String name = "someName" + i;
		client.download(name).whenComplete((supplier, e) -> {
			if (e != null) {
				logger.error("can't download", e);
			} else {
				//				try {
				//					supplier.streamTo(ChannelFileWriter.create(executor, downloads.resolve(name)));
				//				} catch (IOException e) {
				//					logger.error("can't download", e);
				//				}
				throw new UnsupportedOperationException("TODO");
			}
		});

		eventloop.run();
	}

	public static class TestObject {
		@Serialize(order = 0)
		public String name;

		@Serialize(order = 1)
		public InetAddress ip;
	}

}
