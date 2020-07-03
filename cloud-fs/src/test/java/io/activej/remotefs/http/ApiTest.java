package io.activej.remotefs.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.http.RoutingServlet;
import io.activej.http.StubHttpClient;
import io.activej.promise.Promise;
import io.activej.remotefs.FileMetadata;
import io.activej.remotefs.FsClient;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;
import java.util.stream.IntStream;

import static io.activej.common.collection.CollectionUtils.map;
import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.promise.TestUtils.await;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
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

	private final StubHttpClient stubClient = StubHttpClient.of(getServlet());
	private final HttpFsClient client = HttpFsClient.create("http://localhost", stubClient);

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
		List<String> names = asList("file1.txt", "file2.txt", "file3.txt", "file4.txt");
		doTest(client.infoAll(names), names);
	}

	@Test
	public void delete() {
		doTest(client.delete("test"), "test");
	}

	@Test
	public void deleteAll() {
		Set<String> toDelete = set("file1.txt", "file2.txt", "file3.txt");
		doTest(client.deleteAll(toDelete), toDelete);
	}

	@Test
	public void move() {
		doTest(client.move("source", "target"), "source", "target");
	}

	@Test
	public void moveAll() {
		Map<String, String> sourceToTarget = map(
				"file1.txt", "newFile1.txt",
				"file2.txt", "newFile2.txt",
				"file3.txt", "newFile3.txt",
				"file4.txt", "newFile4.txt"
		);
		doTest(client.moveAll(sourceToTarget), sourceToTarget);
	}

	@Test
	public void copy() {
		doTest(client.copy("source", "target"), "source", "target");
	}

	@Test
	public void copyAll() {
		Map<String, String> sourceToTarget = map(
				"file1.txt", "newFile1.txt",
				"file2.txt", "newFile2.txt",
				"file3.txt", "newFile3.txt",
				"file4.txt", "newFile4.txt"
		);
		doTest(client.copyAll(sourceToTarget), sourceToTarget);
	}

	@Test
	public void upload() {
		Promise<Void> uploadPromise = ChannelSupplier.ofIterable(data)
				.map(ByteBufStrings::wrapUtf8)
				.streamTo(client.upload("test"));
		doTest(uploadPromise, "test", data);
	}

	@Test
	public void download() {
		List<String> chunks = new ArrayList<>();
		Promise<Void> uploadPromise = ChannelSupplier.ofPromise(client.download("test", 10 , 20))
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
		Promise<Void> uploadPromise = ChannelSupplier.ofPromise(client.download("test", 10 , Long.MAX_VALUE))
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

	@NotNull
	private RoutingServlet getServlet() {
		return RemoteFsServlet.create(new FsClient() {
			<T> Promise<T> resultOf(@Nullable T result, Object... args) {
				params.clear();
				params.add(result);
				params.addAll(asList(args));
				return Promise.of(result);
			}

			@Override
			public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
				List<String> received = new ArrayList<>();
				return Promise.of(ChannelConsumer.<String>ofConsumer(received::add)
						.<ByteBuf>map(byteBuf -> byteBuf.asString(UTF_8))
						.withAcknowledgement(ack -> ack
								.then(result -> resultOf(result, name, received))));
			}

			@Override
			public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long limit) {
				return Promise.of(ChannelSupplier.ofIterable(data)
						.map(ByteBufStrings::wrapUtf8)
						.withEndOfStream(eos -> eos
								.then(result -> resultOf(result, name, offset, limit, data))));
			}

			@Override
			public Promise<Void> delete(@NotNull String name) {
				return resultOf(null, name);
			}

			@Override
			public Promise<List<FileMetadata>> list(@NotNull String glob) {
				return resultOf(asList(
						FileMetadata.of("test1", 100, 10),
						FileMetadata.of("test2", 200, 20),
						FileMetadata.of("test3", 300, 30)
				), glob);
			}

			@Override
			public Promise<Void> deleteAll(Set<String> toDelete) {
				return resultOf(null, toDelete);
			}

			@Override
			public Promise<Void> copy(@NotNull String name, @NotNull String target) {
				return resultOf(null, name, target);
			}

			@Override
			public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
				return resultOf(null, sourceToTarget);
			}

			@Override
			public Promise<Void> move(@NotNull String name, @NotNull String target) {
				return resultOf(null, name, target);
			}

			@Override
			public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
				return resultOf(null, sourceToTarget);
			}

			@Override
			public Promise<@Nullable FileMetadata> info(@NotNull String name) {
				FileMetadata result = name.equals("nullable") ? null : FileMetadata.of(name, 100, 200);
				return resultOf(result, name);
			}

			@Override
			public Promise<Map<String, @Nullable FileMetadata>> infoAll(@NotNull List<String> names) {
				Map<String, @Nullable FileMetadata> result = new HashMap<>();
				for (int i = 0; i < names.size(); i++) {
					String name = names.get(i);
					result.put(name, i % 2 == 0 ? null : FileMetadata.of(name, i, i * 10));
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
