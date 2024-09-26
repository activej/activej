package io.activej.csp.file;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.MemSize;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.ChannelSuppliers;
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

	private static final Path IN_DAT_PATH = Paths.get("test_data/in.dat");

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void streamFileReader() throws IOException {
		ByteBuf byteBuf = await(ChannelFileReader.open(newCachedThreadPool(), IN_DAT_PATH)
			.then(cfr -> cfr.toCollector(ByteBufs.collector())));

		assertArrayEquals(Files.readAllBytes(IN_DAT_PATH), byteBuf.asArray());
	}

	@Test
	public void streamFileReaderWithDelay() throws IOException {
		ByteBuf byteBuf = await(ChannelFileReader.builderOpen(newCachedThreadPool(), IN_DAT_PATH)
			.then(builder -> builder.withBufferSize(MemSize.of(1))
				.build()
				.mapAsync(buf -> Promises.delay(10L, buf))
				.toCollector(ByteBufs.collector())));

		assertArrayEquals(Files.readAllBytes(IN_DAT_PATH), byteBuf.asArray());
	}

	@Test
	public void streamFileWriter() throws IOException {
		Path tempPath = temporaryFolder.getRoot().toPath().resolve("out.dat");
		byte[] bytes = {'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		await(ChannelSuppliers.ofValue(ByteBuf.wrapForReading(bytes))
			.streamTo(ChannelFileWriter.open(newCachedThreadPool(), tempPath)));

		assertArrayEquals(bytes, Files.readAllBytes(tempPath));
	}

	@Test
	public void streamFileWriterRecycle() {
		Path tempPath = temporaryFolder.getRoot().toPath().resolve("out.dat");
		byte[] bytes = {'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		ChannelFileWriter writer = await(ChannelFileWriter.open(newCachedThreadPool(), tempPath));

		Exception exception = new Exception("Test Exception");

		writer.closeEx(exception);
		Exception e = awaitException(ChannelSuppliers.ofValue(ByteBuf.wrapForReading(bytes))
			.streamTo(writer)
			.then(() -> writer.accept(ByteBuf.wrapForReading("abc".getBytes()))));
		assertSame(exception, e);
	}

	@Test
	public void streamFileReaderWhenFileMultipleOfBuffer() throws IOException {
		Path folder = temporaryFolder.newFolder().toPath();
		byte[] data = new byte[3 * ChannelFileReader.DEFAULT_BUFFER_SIZE.toInt()];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte) (i % 256 - 127);
		}
		Path file = folder.resolve("test.bin");
		Files.write(file, data);

		await(ChannelFileReader.open(newCachedThreadPool(), file)
			.then(cfr -> cfr.streamTo(ChannelConsumers.ofAsyncConsumer(buf -> {
				assertTrue("Received byte buffer is empty", buf.canRead());
				buf.recycle();
				return Promise.complete();
			}))));
	}

	@Test
	public void close() throws Exception {
		File file = temporaryFolder.newFile("2Mb");
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
		ChannelFileReader.Builder builder = await(ChannelFileReader.builderOpen(newCachedThreadPool(), IN_DAT_PATH));

		ByteBuf byteBuf = await(builder.withOffset(Files.size(IN_DAT_PATH) + 100)
			.withBufferSize(MemSize.of(1))
			.build()
			.toCollector(ByteBufs.collector()));

		assertEquals("", byteBuf.asString(UTF_8));
	}
}
