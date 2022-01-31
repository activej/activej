package io.activej.async.file;

import io.activej.promise.Promises;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.activej.promise.TestUtils.await;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class ExecutorAsyncFileServiceTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private final ExecutorAsyncFileService service = new ExecutorAsyncFileService(Executors.newCachedThreadPool());

	static {
		System.setProperty("AsyncFileService.aio", "false");
	}

	@Test
	public void testRead() throws IOException {
		Path srcPath = Paths.get("test_data/test.txt");
		FileChannel channel = FileChannel.open(srcPath, Set.of(READ));

		byte[] result = new byte[20];
		await(Promises.all(IntStream.range(0, 100)
				.mapToObj(i -> service.read(channel, 0, result, 0, result.length)
						.whenComplete((res, e) -> {
							if (e != null) {
								e.printStackTrace();
								fail();
							}

							try {
								assertEquals(Files.readAllBytes(srcPath).length, res.intValue());
							} catch (IOException e1) {
								e1.printStackTrace();
							}
						})).collect(Collectors.toList())));
	}

	@Test
	public void testWrite() throws IOException {
		Path srcPath = Paths.get("test_data/test.txt");
		FileChannel channel = FileChannel.open(srcPath, Set.of(READ, WRITE));
		byte[] array = "Hello world!!!!!".getBytes();

		await(Promises.all(IntStream.range(0, 1000)
				.mapToObj($ -> service.write(channel, 0, array, 0, array.length)
						.whenComplete((res, e) -> {
							if (e != null) {
								e.printStackTrace();
								fail();
							}
							assertEquals(res.intValue(), array.length);
						}))));
	}
}
