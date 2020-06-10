package io.activej.remotefs;

import io.activej.bytebuf.ByteBuf;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static io.activej.promise.TestUtils.await;
import static io.activej.remotefs.RemoteFsUtils.copyFile;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public final class TestFsCopy {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private LocalFsClient sourceClient;
	private LocalFsClient targetClient;

	@Before
	public void setup() throws IOException {
		Executor executor = Executors.newSingleThreadExecutor();
		sourceClient = LocalFsClient.create(Eventloop.getCurrentEventloop(), executor, tmpFolder.newFolder("source").toPath()).withRevisions();
		targetClient = LocalFsClient.create(Eventloop.getCurrentEventloop(), executor,tmpFolder.newFolder("target").toPath()).withRevisions();
	}

		/*
		x - tombstone
		# - existing file
		_ - absent file


		- no source {{
			* do nothing
			   _ -> _
			   _ -> x
			   _ -> #
	    }}

		- source is tombstone {{
			* do nothing
			   x -> x (better)
			   x -> # (better)

			* create the same tombstone on target
			   x -> _
			   x -> x (worse)
			   x -> # (worse)
		}}

		- source is a file {{
			* copy whole file
			   # -> _
			   # -> # (worse)

			* do nothing
			   # -> # (better)

			* do nothing
			   # -> # (same or bigger)

			* copy source tail
			   # -> # (lesser)
	    }}
		*/

	@Test
	public void sourceTombstone() {
		await(sourceClient.delete("a.txt", 10));
		await(sourceClient.delete("b.txt", 10));
		await(sourceClient.delete("c.txt", 5));


		await(targetClient.upload("a.txt", 5)).acceptAll(ByteBuf.wrapForReading("hello".getBytes(UTF_8)), null);
		await(targetClient.upload("b.txt", 15)).acceptAll(ByteBuf.wrapForReading("hello".getBytes(UTF_8)), null);

		await(Promises.sequence(Stream.of("a.txt", "b.txt", "c.txt", "zzz").map(name -> () -> copyFile(sourceClient, targetClient, name))));
		// absent c.txt

		Set<String> res = await(targetClient.listEntities("**")).stream()
				.map(m -> m.getName() + " " + m.getRevision() + " " + m.isTombstone())
				.collect(toSet());

		assertEquals(Stream.of("a.txt 10 true", "b.txt 15 false", "c.txt 5 true").collect(toSet()), res);
	}

	@Test
	public void sourceFile() {
		await(sourceClient.upload("a.txt", 10)).acceptAll(ByteBuf.wrapForReading("hello".getBytes(UTF_8)), null);
		await(sourceClient.upload("b.txt", 10)).acceptAll(ByteBuf.wrapForReading("hello".getBytes(UTF_8)), null);
		await(sourceClient.upload("c.txt", 5)).acceptAll(ByteBuf.wrapForReading("hello".getBytes(UTF_8)), null);
		await(sourceClient.upload("d.txt", 10)).acceptAll(ByteBuf.wrapForReading("hel".getBytes(UTF_8)), null);
		await(sourceClient.upload("e.txt", 10)).acceptAll(ByteBuf.wrapForReading("hello_long".getBytes(UTF_8)), null);


		await(targetClient.upload("a.txt", 5)).acceptAll(ByteBuf.wrapForReading("hello".getBytes(UTF_8)), null);
		await(targetClient.upload("b.txt", 15)).acceptAll(ByteBuf.wrapForReading("hello".getBytes(UTF_8)), null);
		// absent c.txt
		await(sourceClient.upload("d.txt", 10)).acceptAll(ByteBuf.wrapForReading("hello".getBytes(UTF_8)), null);
		await(sourceClient.upload("e.txt", 10)).acceptAll(ByteBuf.wrapForReading("hello".getBytes(UTF_8)), null);

		await(Promises.sequence(Stream.of("a.txt", "b.txt", "c.txt", "d.txt", "e.txt", "zzz").map(name -> () -> copyFile(sourceClient, targetClient, name))));

		Set<String> res = await(targetClient.listEntities("**")).stream()
				.map(m -> m.getName() + " " + m.getRevision() + " " + m.isTombstone() + " " + m.getSize())
				.collect(toSet());

		assertEquals(Stream.of("a.txt 10 false 5", "b.txt 15 false 5", "c.txt 5 false 5", "d.txt 10 false 5", "e.txt 10 false 10").collect(toSet()), res);
	}
}
