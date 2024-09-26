package io.activej.fs;

import io.activej.csp.consumer.ChannelConsumer;
import io.activej.fs.adapter.FileSystemAdapters;
import io.activej.fs.exception.ForbiddenPathException;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public final class FileSystemAdaptersTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private IFileSystem fileSystem;

	@Before
	public void setup() throws IOException {
		Path path = temporaryFolder.newFolder("test").toPath();
		FileSystem fileSystem = FileSystem.create(getCurrentReactor(), newSingleThreadExecutor(), path);
		await(fileSystem.start());
		this.fileSystem = fileSystem;
	}

	private void upload(IFileSystem fs, String filename) {
		await(fs.upload(filename).then(ChannelConsumer::acceptEndOfStream));
	}

	private void uploadForbidden(IFileSystem fs, String filename) {
		Exception exception = awaitException(fs.upload(filename).then(ChannelConsumer::acceptEndOfStream));
		assertThat(exception, instanceOf(ForbiddenPathException.class));
	}

	private void expect(String... realFiles) {
		assertEquals(Arrays.stream(realFiles).collect(toSet()), await(fileSystem.list("**")).keySet());
	}

	@Test
	public void addingPrefix() {
		IFileSystem prefixed = FileSystemAdapters.addPrefix(fileSystem, "prefix/");

		upload(prefixed, "test.txt");
		upload(prefixed, "deeper/test.txt");

		expect("prefix/test.txt", "prefix/deeper/test.txt");
	}

	@Test
	public void strippingPrefix() {
		IFileSystem prefixed = FileSystemAdapters.removePrefix(fileSystem, "prefix/");

		upload(prefixed, "prefix/test.txt");
		upload(prefixed, "prefix/deeper/test.txt");

		expect("test.txt", "deeper/test.txt");

		AssertionError e = assertThrows(AssertionError.class, () -> upload(prefixed, "nonPrefix/test.txt"));
		assertThat(e.getCause(), instanceOf(ForbiddenPathException.class));
	}

	@Test
	public void mountingClient() {
		IFileSystem root = FileSystemAdapters.subdirectory(fileSystem, "root");
		IFileSystem first = FileSystemAdapters.subdirectory(fileSystem, "first");
		IFileSystem second = FileSystemAdapters.subdirectory(fileSystem, "second");
		IFileSystem third = FileSystemAdapters.subdirectory(fileSystem, "third");

		IFileSystem mounted = FileSystemAdapters.mount(root, Map.of(
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
		IFileSystem filtered = FileSystemAdapters.filter(fileSystem, s -> s.endsWith(".txt") && Pattern.compile("\\d{2}").matcher(s).find());

		uploadForbidden(filtered, "test2.txt");
		upload(filtered, "test22.txt");
		uploadForbidden(filtered, "test22.jpg");
		upload(filtered, "123.txt");

		expect("test22.txt", "123.txt");
	}
}
