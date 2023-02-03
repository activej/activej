package io.activej.state;

import io.activej.fs.BlockingFileSystem;
import io.activej.serializer.stream.DiffStreamCodec;
import io.activej.serializer.stream.StreamInput;
import io.activej.serializer.stream.StreamOutput;
import io.activej.state.file.FileNamingScheme;
import io.activej.state.file.FileNamingSchemes;
import io.activej.state.file.FileState;
import io.activej.state.file.FileStateManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class FileStateManagerTest {

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	public static final FileNamingScheme NAMING_SCHEME = FileNamingSchemes.create("", "", "", "", '-');

	private FileStateManager<Integer> manager;

	private BlockingFileSystem fileSystem;

	@Before
	public void setUp() throws Exception {
		Path storage = tmpFolder.newFolder().toPath();
		fileSystem = BlockingFileSystem.create(storage);
		fileSystem.start();

		manager = FileStateManager.<Integer>builder(fileSystem, NAMING_SCHEME)
				.withCodec(new IntegerCodec())
				.build();
	}

	@Test
	public void saveAndLoad() throws IOException {
		long revision = manager.save(100);
		FileState<Integer> loaded = manager.load();

		assertEquals(100, (int) loaded.state());
		assertEquals(revision, loaded.revision());
	}

	@Test
	public void tryLoadNone() throws IOException {
		assertNull(manager.tryLoad());
		assertNull(manager.tryLoad(100, 1L));
	}

	@Test
	public void saveAndLoadWithRevisions() throws IOException {
		manager = FileStateManager.<Integer>builder(fileSystem, NAMING_SCHEME)
				.withCodec(new IntegerCodec())
				.withMaxSaveDiffs(3)
				.build();

		manager.save(100);
		manager.save(101);
		manager.save(110);
		manager.save(150);
		long lastRevision = manager.save(300);

		FileState<Integer> loaded = manager.load();

		assertEquals(300, (int) loaded.state());
		assertEquals(lastRevision, loaded.revision());
	}

	@Test
	public void newRevision() throws IOException {
		assertEquals(1, (long) manager.newRevision());
		long newRevision = manager.newRevision();

		assertEquals(1, newRevision);
		manager.saveSnapshot(123, newRevision);

		assertEquals(2, (long) manager.newRevision());
	}

	@Test
	public void getLastSnapshotRevision() throws IOException {
		manager.saveSnapshot(12, 3L);
		manager.saveSnapshot(13, 123L);
		manager.saveSnapshot(11, 12L);
		manager.saveSnapshot(16, 56L);

		assertEquals(Long.valueOf(123), manager.getLastSnapshotRevision());
	}

	@Test
	public void getLastDiffRevision() throws IOException {
		int maxSaveDiffs = 3;
		manager = FileStateManager.<Integer>builder(fileSystem, NAMING_SCHEME)
				.withCodec(new IntegerCodec())
				.withMaxSaveDiffs(maxSaveDiffs)
				.build();

		Long revision = manager.save(100);
		manager.save(200);
		manager.save(300);
		manager.save(400);
		manager.save(500);

		Long lastDiffRevision = manager.getLastDiffRevision(revision);
		assertEquals(Long.valueOf(revision + maxSaveDiffs), lastDiffRevision);
	}

	@Test
	public void saveAndLoadSnapshot() throws IOException {
		manager.saveSnapshot(123, 10L);
		manager.saveSnapshot(345, 112L);
		manager.saveSnapshot(-3245, 99999L);

		assertEquals(123, (int) manager.loadSnapshot(10L));
		assertEquals(345, (int) manager.loadSnapshot(112L));
		assertEquals(-3245, (int) manager.loadSnapshot(99999L));
	}

	@Test
	public void saveAndLoadDiff() throws IOException {
		manager.saveDiff(100, 10L, 25, 1L);

		int integer = manager.loadDiff(25, 1L, 10L);
		assertEquals(100, integer);
	}

	@Test
	public void uploadsAreAtomic() throws IOException {
		IOException expectedException = new IOException("Failed");
		manager = FileStateManager.<Integer>builder(fileSystem, NAMING_SCHEME)
				.withEncoder((stream, item) -> {
					stream.writeInt(1); // some header
					if (item <= 100) {
						stream.writeInt(item);
					} else {
						throw expectedException;
					}
				})
				.build();

		manager.save(50);
		assertEquals(1, fileSystem.list("**").size());

		manager.save(75);
		assertEquals(2, fileSystem.list("**").size());

		try {
			manager.save(125);
			fail();
		} catch (IOException e) {
			assertSame(expectedException, e);
			assertEquals(2, fileSystem.list("**").size()); // no new files are created
		}
	}

	@Test
	public void tryLoadSnapshotNone() throws IOException {
		long revision = ThreadLocalRandom.current().nextLong();
		try {
			manager.loadSnapshot(revision);
			fail();
		} catch (IOException ignored) {
		}

		assertNull(manager.tryLoadSnapshot(revision));
	}

	@Test
	public void tryLoadDiffNone() throws IOException {
		long revisionFrom = ThreadLocalRandom.current().nextLong();
		long revisionTo = revisionFrom + 1;
		try {
			manager.loadDiff(0, revisionFrom, revisionTo);
			fail();
		} catch (IOException ignored) {
		}

		assertNull(manager.tryLoadDiff(0, revisionFrom, revisionTo));
	}

	@Test
	public void saveArbitraryRevision() throws IOException {
		manager.save(100, 10L);
		manager.save(200, 20L);
		manager.save(150, 30L);

		try {
			manager.save(500, 25L);
			fail();
		} catch (IllegalArgumentException ignored) {
		}

		FileState<Integer> load1 = manager.load();
		assertEquals(150, load1.state().intValue());
		assertEquals(30L, load1.revision());

		assertEquals(100, manager.loadSnapshot(10L).intValue());
	}

	private static class IntegerCodec implements DiffStreamCodec<Integer> {
		@Override
		public Integer decode(StreamInput input) throws IOException {
			return input.readInt();
		}

		@Override
		public void encode(StreamOutput output, Integer item) throws IOException {
			output.writeInt(item);
		}

		@Override
		public Integer decodeDiff(StreamInput input, Integer from) throws IOException {
			return from + input.readInt();
		}

		@Override
		public void encodeDiff(StreamOutput output, Integer from, Integer to) throws IOException {
			output.writeInt(to - from);
		}
	}
}
