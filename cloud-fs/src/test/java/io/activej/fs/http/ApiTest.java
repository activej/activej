package io.activej.fs.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.fs.IFileSystem;
import io.activej.fs.FileMetadata;
import io.activej.http.StubHttpClient;
import io.activej.http.RoutingServlet;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;
import java.util.stream.IntStream;

import static io.activej.promise.TestUtils.await;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class ApiTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final List<String> data = IntStream.rangeClosed(0, 100)
			.mapToObj(i -> "chunk" + i + " ")
			.collect(toList());

	private static final long dataSize = 799;

	private final StubHttpClient stubClient = StubHttpClient.of(getServlet());
	private final FileSystem_HttpClient client = FileSystem_HttpClient.create(getCurrentReactor(), "http://localhost", stubClient);

	private final LinkedList<Object> params = new LinkedList<>();

	@Before
	public void setUp() {
		params.clear();
	}

	@Test
	public void ping() {
		doTest(client.ping());
	}

	@Test
	public void list() {
		doTest(client.list("**"), "**");
	}

	@Test
	public void info() {
		doTest(client.info("test"), "test");
		doTest(client.info("nullable"), "nullable");
	}

	@Test
	public void infoAll() {
		Set<String> names = Set.of("file1.txt", "file2.txt", "file3.txt", "file4.txt");
		doTest(client.infoAll(names), names);
	}

	@Test
	public void delete() {
		doTest(client.delete("test"), "test");
	}

	@Test
	public void deleteAll() {
		Set<String> toDelete = Set.of("file1.txt", "file2.txt", "file3.txt");
		doTest(client.deleteAll(toDelete), toDelete);
	}

	@Test
	public void move() {
		doTest(client.move("source", "target"), "source", "target");
	}

	@Test
	public void moveAll() {
		Map<String, String> sourceToTarget = Map.of(
				"file1.txt", "newFile1.txt",
				"file2.txt", "newFile2.txt",
				"file3.txt", "newFile3.txt",
				"file4.txt", "newFile4.txt");
		doTest(client.moveAll(sourceToTarget), sourceToTarget);
	}

	@Test
	public void copy() {
		doTest(client.copy("source", "target"), "source", "target");
	}

	@Test
	public void copyAll() {
		Map<String, String> sourceToTarget = Map.of(
				"file1.txt", "newFile1.txt",
				"file2.txt", "newFile2.txt",
				"file3.txt", "newFile3.txt",
				"file4.txt", "newFile4.txt");
		doTest(client.copyAll(sourceToTarget), sourceToTarget);
	}

	@Test
	public void upload() {
		Promise<Void> uploadPromise = ChannelSupplier.ofList(data)
				.map(ByteBufStrings::wrapUtf8)
				.streamTo(client.upload("test"));
		doTest(uploadPromise, "test", data);
	}

	@Test
	public void uploadWithSize() {
		Promise<Void> uploadPromise = ChannelSupplier.ofList(data)
				.map(ByteBufStrings::wrapUtf8)
				.streamTo(client.upload("test", dataSize));
		doTest(uploadPromise, "test", dataSize, data);
	}

	@Test
	public void append() {
		Promise<Void> appendPromise = ChannelSupplier.ofList(data)
				.map(ByteBufStrings::wrapUtf8)
				.streamTo(client.append("test", dataSize / 2));
		doTest(appendPromise, "test", dataSize / 2, data);
	}

	@Test
	public void download() {
		List<String> chunks = new ArrayList<>();
		Promise<Void> uploadPromise = ChannelSupplier.ofPromise(client.download("test", 10, 20))
				.map(buf -> buf.asString(UTF_8))
				.streamTo(ChannelConsumer.ofConsumer(chunks::add));
		doTest(uploadPromise, "test", 10L, 20L, chunks);
	}

	@Test
	public void downloadHugeLimit() {
		List<String> chunks = new ArrayList<>();
		Promise<Void> uploadPromise = ChannelSupplier.ofPromise(client.download("test", 0, Integer.MAX_VALUE))
				.map(buf -> buf.asString(UTF_8))
				.streamTo(ChannelConsumer.ofConsumer(chunks::add));
		doTest(uploadPromise, "test", 0L, (long) Integer.MAX_VALUE, chunks);
	}

	@Test
	public void downloadToTheEnd() {
		List<String> chunks = new ArrayList<>();
		Promise<Void> uploadPromise = ChannelSupplier.ofPromise(client.download("test", 0, Long.MAX_VALUE))
				.map(buf -> buf.asString(UTF_8))
				.streamTo(ChannelConsumer.ofConsumer(chunks::add));
		doTest(uploadPromise, "test", 0L, Long.MAX_VALUE, chunks);
	}

	@Test
	public void downloadToTheEndWithOffset() {
		List<String> chunks = new ArrayList<>();
		Promise<Void> uploadPromise = ChannelSupplier.ofPromise(client.download("test", 10, Long.MAX_VALUE))
				.map(buf -> buf.asString(UTF_8))
				.streamTo(ChannelConsumer.ofConsumer(chunks::add));
		doTest(uploadPromise, "test", 10L, Long.MAX_VALUE, chunks);
	}

	private <T> void doTest(Promise<T> promise, Object... parameters) {
		T result = await(promise);
		assertEquals(params.remove(), result);
		for (Object param : parameters) {
			assertEquals(params.remove(), param);
		}
		assertTrue(params.isEmpty());
	}

	private RoutingServlet getServlet() {
		return FileSystemServlet.create(Reactor.getCurrentReactor(), new IFileSystem() {
			<T> Promise<T> resultOf(@Nullable T result, Object... args) {
				params.clear();
				params.add(result);
				params.addAll(List.of(args));
				return Promise.of(result);
			}

			@Override
			public Promise<ChannelConsumer<ByteBuf>> upload(String name) {
				List<String> received = new ArrayList<>();
				return Promise.of(ChannelConsumer.<String>ofConsumer(received::add)
						.<ByteBuf>map(byteBuf -> byteBuf.asString(UTF_8))
						.withAcknowledgement(ack -> ack
								.then(result -> resultOf(result, name, received))));
			}

			@Override
			public Promise<ChannelConsumer<ByteBuf>> upload(String name, long size) {
				List<String> received = new ArrayList<>();
				return Promise.of(ChannelConsumer.<String>ofConsumer(received::add)
						.<ByteBuf>map(byteBuf -> byteBuf.asString(UTF_8))
						.withAcknowledgement(ack -> ack
								.then(result -> resultOf(result, name, size, received))));
			}

			@Override
			public Promise<ChannelConsumer<ByteBuf>> append(String name, long offset) {
				List<String> received = new ArrayList<>();
				return Promise.of(ChannelConsumer.<String>ofConsumer(received::add)
						.<ByteBuf>map(byteBuf -> byteBuf.asString(UTF_8))
						.withAcknowledgement(ack -> ack
								.then(result -> resultOf(result, name, offset, received))));
			}

			@Override
			public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long limit) {
				return Promise.of(ChannelSupplier.ofList(data)
						.map(ByteBufStrings::wrapUtf8)
						.withEndOfStream(eos -> eos
								.then(result -> resultOf(result, name, offset, limit, data))));
			}

			@Override
			public Promise<Void> delete(String name) {
				return resultOf(null, name);
			}

			@Override
			public Promise<Map<String, FileMetadata>> list(String glob) {
				return resultOf(
						Map.of(
								"test1", FileMetadata.of(100, 10),
								"test2", FileMetadata.of(200, 20),
								"test3", FileMetadata.of(300, 30)),
						glob);
			}

			@Override
			public Promise<Void> deleteAll(Set<String> toDelete) {
				return resultOf(null, toDelete);
			}

			@Override
			public Promise<Void> copy(String name, String target) {
				return resultOf(null, name, target);
			}

			@Override
			public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
				return resultOf(null, sourceToTarget);
			}

			@Override
			public Promise<Void> move(String name, String target) {
				return resultOf(null, name, target);
			}

			@Override
			public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
				return resultOf(null, sourceToTarget);
			}

			@Override
			public Promise<@Nullable FileMetadata> info(String name) {
				FileMetadata result = name.equals("nullable") ? null : FileMetadata.of(100, 200);
				return resultOf(result, name);
			}

			@Override
			public Promise<Map<String, FileMetadata>> infoAll(Set<String> names) {
				Map<String, FileMetadata> result = new HashMap<>();
				Iterator<String> iterator = names.iterator();
				for (int i = 0; i < names.size(); i++) {
					String name = iterator.next();
					if (i % 2 != 0) {
						result.put(name, FileMetadata.of(i, i * 10L));
					}
				}
				return resultOf(result, names);
			}

			@Override
			public Promise<Void> ping() {
				return resultOf(null);
			}
		});
	}
}
