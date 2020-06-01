package io.activej.csp.file;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.MemSize;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public final class ChannelFileReaderWriterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void streamFileReader() throws IOException {
		ByteBuf byteBuf = await(ChannelFileReader.open(newCachedThreadPool(), Paths.get("test_data/in.dat"))
				.then(cfr -> cfr.toCollector(ByteBufQueue.collector())));

		assertArrayEquals(Files.readAllBytes(Paths.get("test_data/in.dat")), byteBuf.asArray());
	}

	@Test
	public void streamFileReaderWithDelay() throws IOException {
		ByteBuf byteBuf = await(ChannelFileReader.open(newCachedThreadPool(), Paths.get("test_data/in.dat"))
				.then(cfr -> cfr.withBufferSize(MemSize.of(1))
						.mapAsync(buf -> Promises.delay(10L, buf))
						.toCollector(ByteBufQueue.collector())));

		assertArrayEquals(Files.readAllBytes(Paths.get("test_data/in.dat")), byteBuf.asArray());
	}

	@Test
	public void streamFileWriter() throws IOException {
		Path tempPath = tempFolder.getRoot().toPath().resolve("out.dat");
		byte[] bytes = {'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		await(ChannelSupplier.of(ByteBuf.wrapForReading(bytes))
				.streamTo(ChannelFileWriter.open(newCachedThreadPool(), tempPath)));

		assertArrayEquals(bytes, Files.readAllBytes(tempPath));
	}

	@Test
	public void streamFileWriterRecycle() {
		Path tempPath = tempFolder.getRoot().toPath().resolve("out.dat");
		byte[] bytes = {'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		ChannelFileWriter writer = await(ChannelFileWriter.open(newCachedThreadPool(), tempPath));

		Exception exception = new Exception("Test Exception");

		writer.closeEx(exception);
		Throwable e = awaitException(ChannelSupplier.of(ByteBuf.wrapForReading(bytes))
				.streamTo(writer)
				.then(() -> writer.accept(ByteBuf.wrapForReading("abc".getBytes()))));
		assertSame(exception, e);
	}

	@Test
	public void streamFileReaderWhenFileMultipleOfBuffer() throws IOException {
		Path folder = tempFolder.newFolder().toPath();
		byte[] data = new byte[3 * ChannelFileReader.DEFAULT_BUFFER_SIZE.toInt()];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte) (i % 256 - 127);
		}
		Path file = folder.resolve("test.bin");
		Files.write(file, data);

		await(ChannelFileReader.open(newCachedThreadPool(), file)
				.then(cfr -> cfr.streamTo(ChannelConsumer.of(buf -> {
					assertTrue("Received byte buffer is empty", buf.canRead());
					buf.recycle();
					return Promise.complete();
				}))));
	}

	@Test
	public void close() throws Exception {
		File file = tempFolder.newFile("2Mb");
		byte[] data = new byte[2 * 1024 * 1024]; // the larger the file the less chance that it will be read fully before close completes
		ThreadLocalRandom.current().nextBytes(data);

		Path srcPath = file.toPath();
		Files.write(srcPath, data);
		Exception testException = new Exception("Test Exception");

		ChannelFileReader serialFileReader = await(ChannelFileReader.open(newCachedThreadPool(), srcPath));

		serialFileReader.closeEx(testException);
		assertSame(testException, awaitException(serialFileReader.toList()));
	}

	@Test
	public void readOverFile() throws IOException {
		ChannelFileReader cfr = await(ChannelFileReader.open(newCachedThreadPool(), Paths.get("test_data/in.dat")));

		ByteBuf byteBuf = await(cfr.withOffset(Files.size(Paths.get("test_data/in.dat")) + 100)
				.withBufferSize(MemSize.of(1))
				.toCollector(ByteBufQueue.collector()));

		assertEquals("", byteBuf.asString(UTF_8));
	}
}
