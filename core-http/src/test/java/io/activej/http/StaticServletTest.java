package io.activej.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.http.loader.IStaticLoader;
import io.activej.reactor.Reactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.http.loader.IStaticLoader.ofClassPath;
import static io.activej.http.loader.IStaticLoader.ofPath;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.assertEquals;

public final class StaticServletTest {
	public static final String EXPECTED_CONTENT = "Test";

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final TemporaryFolder tmpFolder = new TemporaryFolder();

	private static Path resourcesPath;

	@BeforeClass
	public static void setup() throws IOException {
		resourcesPath = tmpFolder.newFolder("static").toPath();

		Files.write(resourcesPath.resolve("index.html"), encodeAscii(EXPECTED_CONTENT));
	}

	@Test
	public void testPathLoader() {
		Reactor reactor = getCurrentReactor();
		StaticServlet staticServlet = StaticServlet.create(reactor, ofPath(reactor, newCachedThreadPool(), resourcesPath));
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/index.html").build()));
		await(response.loadBody());
		ByteBuf body = response.getBody();

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testFileNotFoundPathLoader() {
		Reactor reactor = getCurrentReactor();
		StaticServlet staticServlet = StaticServlet.create(reactor, ofPath(reactor, newCachedThreadPool(), resourcesPath));
		HttpError e = awaitException(staticServlet.serve(HttpRequest.get("http://test.com:8080/unknownFile.txt").build()));

		assertEquals(404, e.getCode());
	}

	@Test
	public void testClassPath() {
		Reactor reactor = getCurrentReactor();
		StaticServlet staticServlet = StaticServlet.create(reactor, ofClassPath(reactor, newCachedThreadPool(), "/"));
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/testFile.txt").build()));
		await(response.loadBody());
		ByteBuf body = response.getBody();

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testFileNotFoundClassPath() {
		Reactor reactor = getCurrentReactor();
		StaticServlet staticServlet = StaticServlet.create(reactor, ofClassPath(reactor, newCachedThreadPool(), "/"));
		HttpError e = awaitException(staticServlet.serve(HttpRequest.get("http://test.com:8080/index.html").build()));

		assertEquals(404, e.getCode());
	}

	@Test
	public void testRelativeClassPath() {
		Reactor reactor = getCurrentReactor();
		StaticServlet staticServlet = StaticServlet.create(reactor, ofClassPath(reactor, newCachedThreadPool(), getClass().getClassLoader(), "/"));
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/testFile.txt").build()));
		await(response.loadBody());
		ByteBuf body = response.getBody();

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testRelativeClassPathWithInnerPath() {
		Reactor reactor = getCurrentReactor();
		IStaticLoader resourceLoader = ofClassPath(reactor, newCachedThreadPool(), getClass().getClassLoader(), "/dir/");
		StaticServlet staticServlet = StaticServlet.create(reactor, resourceLoader);
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/test.txt").build()));
		await(response.loadBody());
		ByteBuf body = response.getBody();

		assertEquals(EXPECTED_CONTENT, body.asString(UTF_8));
	}

	@Test
	public void testFileNotFoundRelativeClassPath() {
		Reactor reactor = getCurrentReactor();
		IStaticLoader resourceLoader = ofClassPath(reactor, newCachedThreadPool(), getClass().getClassLoader(), "/");
		StaticServlet staticServlet = StaticServlet.create(reactor, resourceLoader);
		HttpError e = awaitException(staticServlet.serve(HttpRequest.get("http://test.com:8080/unknownFile.txt").build()));

		assertEquals(404, e.getCode());
	}

	@Test
	public void testCustomContentType() throws IOException {
		String customExtension = "cstm";
		String customContent = "Content of custom file";
		String filename = "my-file." + customExtension;
		Files.write(resourcesPath.resolve(filename), encodeAscii(customContent));

		String customType = "test/custom-type";
		MediaTypes.register(customType, customExtension);

		Reactor reactor = getCurrentReactor();
		StaticServlet staticServlet = StaticServlet.create(reactor, ofPath(reactor, newCachedThreadPool(), resourcesPath));
		HttpResponse response = await(staticServlet.serve(HttpRequest.get("http://test.com:8080/" + filename).build()));
		await(response.loadBody());
		ByteBuf body = response.getBody();

		assertEquals(customContent, body.asString(UTF_8));
		assertEquals(customType, response.getHeader(HttpHeaders.CONTENT_TYPE));
	}
}
