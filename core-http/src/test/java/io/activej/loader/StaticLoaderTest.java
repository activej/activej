package io.activej.loader;

import io.activej.bytebuf.ByteBuf;
import io.activej.http.loader.IStaticLoader;
import io.activej.http.loader.ResourceNotFoundException;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.file.Paths;

import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StaticLoaderTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testMap() {
		IStaticLoader staticLoader = IStaticLoader.ofClassPath(getCurrentReactor(), newCachedThreadPool(), "/")
			.map(file -> file + ".txt");
		ByteBuf file = await(staticLoader.load("testFile"));
		assertTrue(file.readRemaining() > 0);
	}

	@Test
	public void testFileNotFoundClassPath() {
		IStaticLoader staticLoader = IStaticLoader.ofClassPath(getCurrentReactor(), newCachedThreadPool(), "/");
		Exception exception = awaitException(staticLoader.load("unknownFile.txt"));
		assertThat(exception, instanceOf(ResourceNotFoundException.class));
	}

	@Test
	public void testFileNotFoundPath() {
		IStaticLoader staticLoader = IStaticLoader.ofPath(getCurrentReactor(), newCachedThreadPool(), Paths.get("/"));
		Exception exception = awaitException(staticLoader.load("unknownFile.txt"));
		assertThat(exception, instanceOf(ResourceNotFoundException.class));
	}

	@Test
	public void testLoadClassPathFile() {
		IStaticLoader staticLoader = IStaticLoader.ofClassPath(getCurrentReactor(), newCachedThreadPool(), "/");
		ByteBuf file = await(staticLoader.load("testFile.txt"));
		assertNotNull(file);
		assertTrue(file.readRemaining() > 0);
	}

	@Test
	public void testFilterFileClassPath() {
		IStaticLoader staticLoader = IStaticLoader.ofClassPath(getCurrentReactor(), newCachedThreadPool(), "/")
			.filter(file -> !file.equals("testFile.txt"));
		Exception exception = awaitException(staticLoader.load("testFile.txt"));
		assertThat(exception, instanceOf(ResourceNotFoundException.class));
	}

	@Test
	public void testClassPathWithDiffRoot() {
		IStaticLoader staticLoader = IStaticLoader.ofClassPath(getCurrentReactor(), newCachedThreadPool(), "/");
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
		IStaticLoader staticLoader = IStaticLoader.ofPath(getCurrentReactor(), newCachedThreadPool(), Paths.get("/"))
			.filter(file -> !file.equals("testFile.txt"));
		Exception exception = awaitException(staticLoader.load("testFile.txt"));
		assertThat(exception, instanceOf(ResourceNotFoundException.class));
	}

	@Test
	public void testClassPathWithDir() {
		IStaticLoader staticLoader = IStaticLoader.ofClassPath(getCurrentReactor(), newCachedThreadPool(), "dir");
		ByteBuf file = await(staticLoader.load("test.txt"));
		assertNotNull(file);
	}
}
