package io.activej.remotefs;

import io.activej.bytebuf.ByteBufQueue;
import io.activej.csp.ChannelSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static io.activej.bytebuf.ByteBufStrings.wrapUtf8;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.remotefs.FsClient.OFFSET_TOO_BIG;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.*;

public final class TestLocalFsClientRevisions {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private FsClient client;

	@Before
	public void setUp() throws IOException {
		client = LocalFsClient.create(Eventloop.getCurrentEventloop(), newSingleThreadExecutor(), tmpFolder.newFolder("storage").toPath()).withRevisions();
	}

	@Test
	public void uploadOverride() {
		await(ChannelSupplier.of(wrapUtf8("hello, this is first text")).streamTo(client.upload("test.txt", 0, 1)));
		await(ChannelSupplier.of(wrapUtf8("OVERRIDEN")).streamTo(client.upload("test.txt", 0, 2)));

		assertEquals("OVERRIDEN", download("test.txt"));
	}

	@Test
	public void uploadAppend() {

		await(ChannelSupplier.of(wrapUtf8("hello, this is first text")).streamTo(client.upload("test.txt", 0, 123)));
		await(ChannelSupplier.of(wrapUtf8("rst text and some appended text too")).streamTo(client.upload("test.txt", 17, 123)));

		assertEquals("hello, this is first text and some appended text too", download("test.txt"));
	}

	@Test
	public void uploadOverrideAppend() {
		await(ChannelSupplier.of(wrapUtf8("hello, this is first text")).streamTo(client.upload("test.txt", 0, 1)));

		Promise<Void> process = ChannelSupplier.of(wrapUtf8("rst text and some appended text too")).streamTo(client.upload("test.txt", 17, 2));
		assertSame(OFFSET_TOO_BIG, awaitException(process));
	}

	@Test
	public void uploadDeleteUpload() {
		await(ChannelSupplier.of(wrapUtf8("hello, this is first text")).streamTo(client.upload("test.txt", 0, 1)));
		await(client.delete("test.txt", 1));

		FileMetadata metadata = await(client.getMetadata("test.txt"));
		assertNotNull(metadata);
		assertTrue(metadata.isTombstone());

		await(ChannelSupplier.of(wrapUtf8("OVERRIDEN")).streamTo(client.upload("test.txt", 0, 2)));

		metadata = await(client.getMetadata("test.txt"));
		assertNotNull(metadata);
		assertFalse(metadata.isTombstone());

		assertEquals("OVERRIDEN", download("test.txt"));
	}

	@Test
	public void lowRevisionDelete() {
		await(ChannelSupplier.of(wrapUtf8("hello, this is first text")).streamTo(client.upload("test.txt", 0, 10)));

		await(client.delete("test.txt", 1));

		FileMetadata metadata = await(client.getMetadata("test.txt"));
		assertNotNull(metadata);
		assertFalse(metadata.isTombstone());
	}

	@Test
	public void deleteBeforeUpload() {
		await(client.delete("test.txt", 10));

		await(ChannelSupplier.of(wrapUtf8("hello, this is first text")).streamTo(client.upload("test.txt", 0, 1)));

		FileMetadata metadata = await(client.getMetadata("test.txt"));
		assertNotNull(metadata);
		assertTrue(metadata.isTombstone());
	}

	@Test
	public void moveIntoLesser() {
		await(ChannelSupplier.of(wrapUtf8("hello, this is some text")).streamTo(client.upload("test.txt", 0, 1)));
		await(ChannelSupplier.of(wrapUtf8("and this is another")).streamTo(client.upload("test2.txt", 0, 1)));

		await(client.move("test.txt", "test2.txt", 2, 2));

		FileMetadata metadata = await(client.getMetadata("test.txt"));
		assertNotNull(metadata);
		assertTrue(metadata.isTombstone());

		System.out.println(await(client.listEntities("**")));
		assertEquals("hello, this is some text", download("test2.txt"));
	}

	@Test
	public void moveIntoHigher() {
		await(ChannelSupplier.of(wrapUtf8("hello, this is some text")).streamTo(client.upload("test.txt", 0, 1)));
		await(ChannelSupplier.of(wrapUtf8("and this is another")).streamTo(client.upload("test2.txt", 0, 10)));

		await(client.move("test.txt", "test2.txt", 2, 2));
		assertEquals("and this is another", download("test2.txt"));
	}

	@Test
	public void copyIntoLesser() {
		await(ChannelSupplier.of(wrapUtf8("hello, this is some text")).streamTo(client.upload("test.txt", 0, 1)));
		await(ChannelSupplier.of(wrapUtf8("and this is another")).streamTo(client.upload("test2.txt", 0, 1)));

		await(client.copy("test.txt", "test2.txt", 2));

		assertEquals("hello, this is some text", download("test2.txt"));
		assertEquals("hello, this is some text", download("test.txt"));
	}

	@Test
	public void copyIntoHigher() {
		await(ChannelSupplier.of(wrapUtf8("hello, this is some text")).streamTo(client.upload("test.txt", 0, 1)));
		await(ChannelSupplier.of(wrapUtf8("and this is another")).streamTo(client.upload("test2.txt", 0, 10)));

		await(client.copy("test.txt", "test2.txt", 2));

		assertEquals("and this is another", download("test2.txt"));
	}

	private String download(String name) {
		return await(await(client.download(name)).toCollector(ByteBufQueue.collector())).asString(UTF_8);
	}
}
