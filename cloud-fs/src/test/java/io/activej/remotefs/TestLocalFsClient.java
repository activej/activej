package io.activej.remotefs;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.MemSize;
import io.activej.common.exception.ExpectedException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.ChannelSuppliers;
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.eventloop.Eventloop;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.csp.binary.BinaryChannelSupplier.UNEXPECTED_DATA_EXCEPTION;
import static io.activej.csp.binary.BinaryChannelSupplier.UNEXPECTED_END_OF_STREAM_EXCEPTION;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.remotefs.FsClient.*;
import static io.activej.remotefs.LocalFsClient.SYSTEM_DIR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.*;

public final class TestLocalFsClient {
	private static final MemSize BUFFER_SIZE = MemSize.of(2);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private Path storagePath;
	private Path clientPath;

	private LocalFsClient client;

	@Before
	public void setup() throws IOException {
		storagePath = tmpFolder.newFolder("storage").toPath();
		clientPath = tmpFolder.newFolder("client").toPath();

		Files.createDirectories(storagePath);
		Files.createDirectories(clientPath);

		Path f = clientPath.resolve("f.txt");
		Files.write(f, "some text1\n\nmore text1\t\n\n\r".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path c = clientPath.resolve("c.txt");
		Files.write(c, "some text2\n\nmore text2\t\n\n\r".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Files.createDirectories(storagePath.resolve("1"));
		Files.createDirectories(storagePath.resolve("2/3"));
		Files.createDirectories(storagePath.resolve("2/b"));

		Path a1 = storagePath.resolve("1/a.txt");
		Files.write(a1, "1\n2\n3\n4\n5\n6\n".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path b = storagePath.resolve("1/b.txt");
		Files.write(b, "7\n8\n9\n10\n11\n12\n".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path a2 = storagePath.resolve("2/3/a.txt");
		Files.write(a2, "6\n5\n4\n3\n2\n1\n".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path d = storagePath.resolve("2/b/d.txt");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1_000_000; i++) {
			sb.append(i).append("\n");
		}
		Files.write(d, sb.toString().getBytes(UTF_8));

		Path e = storagePath.resolve("2/b/e.txt");
		try {
			Files.createFile(e);
		} catch (IOException ignored) {
		}

		client = LocalFsClient.create(Eventloop.getCurrentEventloop(), newSingleThreadExecutor(), storagePath);
	}

	@Test
	public void testDoUpload() throws IOException {
		Path path = clientPath.resolve("c.txt");

		await(client.upload("1/c.txt")
				.then(consumer -> ChannelFileReader.open(newCachedThreadPool(), path)
						.then(file -> file.withBufferSize(BUFFER_SIZE).streamTo(consumer))));

		assertArrayEquals(Files.readAllBytes(path), Files.readAllBytes(storagePath.resolve("1/c.txt")));
	}

	@Test
	public void uploadIncompleteFile() {
		String filename = "incomplete.txt";
		Path path = storagePath.resolve(filename);
		assertFalse(Files.exists(path));

		ExpectedException expectedException = new ExpectedException();
		ChannelConsumer<ByteBuf> consumer = await(client.upload(filename));

		Throwable exception = awaitException(ChannelSuppliers.concat(
				ChannelSupplier.of(wrapUtf8("some"), wrapUtf8("test"), wrapUtf8("data")),
				ChannelSupplier.ofException(expectedException))
				.streamTo(consumer));

		assertSame(expectedException, exception);

		assertFalse(Files.exists(path));
	}

	@Test
	public void uploadLessThanSpecified() {
		String filename = "incomplete.txt";
		Path path = storagePath.resolve(filename);
		assertFalse(Files.exists(path));

		ChannelConsumer<ByteBuf> consumer = await(client.upload(filename, 10));

		Throwable exception = awaitException(ChannelSupplier.of(wrapUtf8("data")).streamTo(consumer));

		assertEquals(UNEXPECTED_END_OF_STREAM_EXCEPTION, exception);

		assertFalse(Files.exists(path));
	}

	@Test
	public void uploadMoreThanSpecified() {
		String filename = "incomplete.txt";
		Path path = storagePath.resolve(filename);
		assertFalse(Files.exists(path));

		ChannelConsumer<ByteBuf> consumer = await(client.upload(filename, 10));

		Throwable exception = awaitException(ChannelSupplier.of(wrapUtf8("data data data data")).streamTo(consumer));

		assertEquals(UNEXPECTED_DATA_EXCEPTION, exception);

		assertFalse(Files.exists(path));
	}

	@Test
	public void testDoDownload() throws IOException {
		Path outputFile = clientPath.resolve("d.txt");

		ChannelSupplier<ByteBuf> supplier = await(client.download("2/b/d.txt"));
		await(supplier.streamTo(ChannelFileWriter.open(newCachedThreadPool(), outputFile)));

		assertArrayEquals(Files.readAllBytes(storagePath.resolve("2/b/d.txt")), Files.readAllBytes(outputFile));
	}

	@Test
	public void testDownloadNonExistingFile() {
		Throwable e = awaitException(client.download("no_file.txt"));

		assertSame(FILE_NOT_FOUND, e);
	}

	@Test
	public void testDeleteFile() {
		await(client.delete("2/3/a.txt"));

		assertFalse(Files.exists(storagePath.resolve("2/3/a.txt")));
	}

	@Test
	public void testDeleteNonExistingFile() {
		await(client.delete("no_file.txt"));
	}

	@Test
	public void testListFiles() {
		Set<String> expected = set(
				"1/a.txt",
				"1/b.txt",
				"2/3/a.txt",
				"2/b/d.txt",
				"2/b/e.txt"
		);

		Map<String, FileMetadata> actual = await(client.list("**"));

		assertEquals(expected, actual.keySet());
	}

	@Test
	public void testGlobListFiles() {
		Set<String> expected = set(
				"2/3/a.txt",
				"2/b/d.txt",
				"2/b/e.txt"
		);

		Map<String, FileMetadata> actual = await(client.list("2/*/*.txt"));

		assertEquals(expected, actual.keySet());
	}

	@Test
	public void testMove() throws IOException {
		byte[] expected = Files.readAllBytes(storagePath.resolve("1/a.txt"));
		await(client.move("1/a.txt", "3/new_folder/z.txt"));

		assertArrayEquals(expected, Files.readAllBytes(storagePath.resolve("3/new_folder/z.txt")));
		assertFalse(Files.exists(storagePath.resolve("1/a.txt")));
	}

	@Test
	public void testMoveIntoExisting() {
		Throwable exception = awaitException(client.move("1/b.txt", "1/a.txt"));
		assertSame(FILE_EXISTS, exception);
	}

	@Test
	public void testMoveNothingIntoNothing() {
		Throwable exception = awaitException(client.move("i_do_not_exist.txt", "neither_am_i.txt"));

		assertEquals(FILE_NOT_FOUND, exception);
	}

	@Test
	public void testOverwritingDirAsFile() {
		await(ChannelSupplier.of(wrapUtf8("test")).streamTo(client.upload("newdir/a.txt")));
		await(client.delete("newdir/a.txt"));

		assertTrue(await(client.list("**")).keySet().stream().noneMatch(name -> name.contains("newdir")));
		await(ChannelSupplier.of(wrapUtf8("test")).streamTo(client.upload("newdir")));
		assertNotNull(await(client.info("newdir")));
	}

	@Test
	public void testDeleteEmpty() {
		await(client.delete(""));
	}

	@Test
	public void testListMalformedGlob() {
		Throwable exception = awaitException(client.list("["));
		assertSame(MALFORMED_GLOB, exception);
	}

	@Test
	public void systemFilesAreNotListed() throws IOException {
		Map<String, FileMetadata> before = await(client.list("**"));

		Path systemDir = storagePath.resolve(SYSTEM_DIR);
		Files.createDirectories(systemDir);
		Files.write(systemDir.resolve("systemFile.txt"), "test data".getBytes());
		Path folder = systemDir.resolve("folder");
		Files.createDirectories(folder);
		Files.write(folder.resolve("systemFile2.txt"), "test data".getBytes());

		Map<String, FileMetadata> after = await(client.list("**"));

		assertEquals(before, after);
	}
}
