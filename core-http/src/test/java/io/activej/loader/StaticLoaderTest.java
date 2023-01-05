package io.activej.loader;

import io.activej.bytebuf.ByteBuf;
import io.activej.http.loader.ResourceNotFoundException;
import io.activej.http.loader.AsyncStaticLoader;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.file.Paths;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StaticLoaderTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testMap() {
		AsyncStaticLoader staticLoader = AsyncStaticLoader.ofClassPath(newCachedThreadPool(), "/")
				.map(file -> file + ".txt");
		ByteBuf file = await(staticLoader.load("testFile"));
		assertTrue(file.readRemaining() > 0);
	}

	@Test
	public void testFileNotFoundClassPath() {
		AsyncStaticLoader staticLoader = AsyncStaticLoader.ofClassPath(newCachedThreadPool(), "/");
		Exception exception = awaitException(staticLoader.load("unknownFile.txt"));
		assertThat(exception, instanceOf(ResourceNotFoundException.class));
	}

	@Test
	public void testFileNotFoundPath() {
		AsyncStaticLoader staticLoader = AsyncStaticLoader.ofPath(newCachedThreadPool(), Paths.get("/"));
		Exception exception = awaitException(staticLoader.load("unknownFile.txt"));
		assertThat(exception, instanceOf(ResourceNotFoundException.class));
	}

	@Test
	public void testLoadClassPathFile() {
		AsyncStaticLoader staticLoader = AsyncStaticLoader.ofClassPath(newCachedThreadPool(), "/");
		ByteBuf file = await(staticLoader.load("testFile.txt"));
		assertNotNull(file);
		assertTrue(file.readRemaining() > 0);
	}

	@Test
	public void testFilterFileClassPath() {
		AsyncStaticLoader staticLoader = AsyncStaticLoader.ofClassPath(newCachedThreadPool(), "/")
				.filter(file -> !file.equals("testFile.txt"));
		Exception exception = awaitException(staticLoader.load("testFile.txt"));
		assertThat(exception, instanceOf(ResourceNotFoundException.class));
	}

	@Test
	public void testClassPathWithDiffRoot() {
		AsyncStaticLoader staticLoader = AsyncStaticLoader.ofClassPath(newCachedThreadPool(), "/");
		ByteBuf buf = await(staticLoader.load("/testFile.txt"));
		assertNotNull(buf);
		buf = await(staticLoader.load("/testFile.txt/"));
		assertNotNull(buf);
		buf = await(staticLoader.load("testFile.txt/"));
		assertNotNull(buf);
		buf = await(staticLoader.load("testFile.txt"));
		assertNotNull(buf);
	}

	@Test
	public void testFilterFilePath() {
		AsyncStaticLoader staticLoader = AsyncStaticLoader.ofPath(newCachedThreadPool(), Paths.get("/"))
				.filter(file -> !file.equals("testFile.txt"));
		Exception exception = awaitException(staticLoader.load("testFile.txt"));
		assertThat(exception, instanceOf(ResourceNotFoundException.class));
	}

	@Test
	public void testClassPathWithDir() {
		AsyncStaticLoader staticLoader = AsyncStaticLoader.ofClassPath(newCachedThreadPool(), "dir");
		ByteBuf file = await(staticLoader.load("test.txt"));
		assertNotNull(file);
	}
}
