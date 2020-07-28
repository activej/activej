package io.activej.fs;

import io.activej.csp.ChannelConsumer;
import io.activej.eventloop.Eventloop;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import static io.activej.common.collection.CollectionUtils.map;
import static io.activej.fs.ActiveFs.BAD_PATH;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public final class ActiveFsAdaptersTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private ActiveFs local;

	@Before
	public void setup() throws IOException {
		local = LocalActiveFs.create(Eventloop.getCurrentEventloop(), newSingleThreadExecutor(), temporaryFolder.newFolder("test").toPath());
	}

	private void upload(ActiveFs fs, String filename) {
		await(fs.upload(filename).then(ChannelConsumer::acceptEndOfStream));
	}

	private void uploadFails(ActiveFs fs, String filename, Throwable exception) {
		Throwable throwable = awaitException(fs.upload(filename).then(ChannelConsumer::acceptEndOfStream));
		assertEquals(exception, throwable);
	}

	private void expect(String... realFiles) {
		assertEquals(Arrays.stream(realFiles).collect(toSet()), await(local.list("**")).keySet());
	}

	@Test
	public void addingPrefix() {
		ActiveFs prefixed = ActiveFsAdapters.addPrefix(local, "prefix/");

		upload(prefixed, "test.txt");
		upload(prefixed, "deeper/test.txt");

		expect("prefix/test.txt", "prefix/deeper/test.txt");
	}

	@Test
	public void strippingPrefix() {
		ActiveFs prefixed = ActiveFsAdapters.removePrefix(local, "prefix/");

		upload(prefixed, "prefix/test.txt");
		upload(prefixed, "prefix/deeper/test.txt");

		expect("test.txt", "deeper/test.txt");

		try {
			upload(prefixed, "nonPrefix/test.txt");
			fail("should've failed");
		} catch (AssertionError e) {
			assertSame(BAD_PATH, e.getCause());
		}
	}

	@Test
	public void mountingClient() {
		ActiveFs root = ActiveFsAdapters.subdirectory(local, "root");
		ActiveFs first = ActiveFsAdapters.subdirectory(local, "first");
		ActiveFs second = ActiveFsAdapters.subdirectory(local, "second");
		ActiveFs third = ActiveFsAdapters.subdirectory(local, "third");

		ActiveFs mounted = ActiveFsAdapters.mount(root, map(
				"hello", first,
				"test/inner", second,
				"last", third));

		//   /           ->  /root
		//   /hello      ->  /first
		//   /test/inner ->  /second
		//   /last       ->  /third

		upload(mounted, "test1.txt");
		upload(mounted, "hello/test2.txt");
		upload(mounted, "test/test3.txt");
		upload(mounted, "test/inner/test4.txt");
		upload(mounted, "last/test5.txt");

		expect("root/test1.txt", "first/test2.txt", "root/test/test3.txt", "second/test4.txt", "third/test5.txt");
	}

	@Test
	public void filterClient() {
		ActiveFs filtered = ActiveFsAdapters.filter(local, s -> s.endsWith(".txt") && Pattern.compile("\\d{2}").matcher(s).find());

		uploadFails(filtered, "test2.txt", BAD_PATH);
		upload(filtered, "test22.txt");
		uploadFails(filtered, "test22.jpg", BAD_PATH);
		upload(filtered, "123.txt");

		expect("test22.txt", "123.txt");
	}
}
