package io.activej.state;

import io.activej.fs.BlockingFileSystem;
import io.activej.serializer.stream.DiffStreamCodec;
import io.activej.serializer.stream.StreamInput;
import io.activej.serializer.stream.StreamOutput;
import io.activej.state.IStateLoader.StateWithRevision;
import io.activej.state.file.FileNamingScheme;
import io.activej.state.file.FileNamingSchemes;
import io.activej.state.file.FileStateManager;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class FileStateManagerTest {

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private FileStateManager<Long, Integer> manager;

	private BlockingFileSystem fileSystem;

	@Parameter()
	public String testName;

	@Parameter(1)
	public FileNamingScheme<Long> fileNamingScheme;

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return List.of(
			new Object[]{"Long file naming scheme", FileNamingSchemes.ofLong("", "")},
			new Object[]{"Long file naming scheme with diffs", FileNamingSchemes.ofLong("", "", "", "", "-")},

			new Object[]{"Long file naming scheme with prefix and suffix", FileNamingSchemes.ofLong("snapshotPrefix", "snapshotSuffix")},
			new Object[]{"Long file naming scheme with diffs, prefix and suffix", FileNamingSchemes.ofLong("snapshotPrefix", "snapshotSuffix", "diffPrefix", "diffSuffix", "-")},

			new Object[]{"Long file naming scheme with directories", FileNamingSchemes.ofLong("snapshots/", "")},
			new Object[]{"Long file naming scheme with diffs and directories", FileNamingSchemes.ofLong("snapshots/", "", "diffs/", "", "-")},
			new Object[]{"Long file naming scheme with diffs and nested directories", FileNamingSchemes.ofLong("a/snapshots/", "", "b/diffs/", "", "-")},
			new Object[]{"Long file naming scheme with diffs and nested within one another directories", FileNamingSchemes.ofLong("snapshots/", "", "snapshots/diffs/", "", "-")}
		);
	}

	@Before
	public void setUp() throws Exception {
		Path storage = tmpFolder.newFolder().toPath();
		fileSystem = BlockingFileSystem.create(storage);
		fileSystem.start();

		manager = FileStateManager.<Long, Integer>builder(fileSystem, fileNamingScheme)
			.withCodec(new IntegerCodec())
			.build();
	}

	@Test
	public void loadEmptyState() throws IOException {
		StateWithRevision<Long, Integer> loaded = manager.load();
		assertNull(loaded);
	}

	@Test
	public void loadEmptyStateWithPrevious() {
		assertThrows(IOException.class, () -> manager.load(1, 1L));
	}

	@Test
	public void loadStateNoChanges() throws IOException {
		manager.save(100);
		StateWithRevision<Long, Integer> loaded = manager.load();
		assertNotNull(loaded);

		StateWithRevision<Long, Integer> loaded2 = manager.load(loaded.state(), loaded.revision());
		assertNull(loaded2);
	}

	@Test
	public void loadInvalidState() throws IOException {
		manager.save(100);
		manager.save(200);
		manager.save(300);
		StateWithRevision<Long, Integer> loaded = manager.load();
		assertNotNull(loaded);

		String snapshot = fileNamingScheme.encodeSnapshot(loaded.revision());
		fileSystem.delete(snapshot);

		assertThrows(IOException.class, () -> manager.load(loaded.state(), loaded.revision()));
	}

	@Test
	public void saveAndLoad() throws IOException {
		long revision = manager.save(100);
		StateWithRevision<Long, Integer> loaded = manager.load();
		assertNotNull(loaded);

		assertEquals(100, loaded.state().intValue());
		assertEquals(revision, loaded.revision().longValue());
	}

	@Test
	public void saveAndLoadWithRevisions() throws IOException {
		manager = FileStateManager.<Long, Integer>builder(fileSystem, fileNamingScheme)
			.withCodec(new IntegerCodec())
			.withMaxSaveDiffs(3)
			.build();

		manager.save(100);
		manager.save(101);
		manager.save(110);
		manager.save(150);
		long lastRevision = manager.save(300);

		//noinspection DataFlowIssue
		long lastSnapshotRevision = manager.getLastSnapshotRevision();
		Integer loaded = manager.loadSnapshot(lastSnapshotRevision);

		assertEquals(300, (int) loaded);
		assertEquals(lastRevision, lastSnapshotRevision);
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
		Assume.assumeTrue(fileNamingScheme.hasDiffsSupport());

		int maxSaveDiffs = 3;
		manager = FileStateManager.<Long, Integer>builder(fileSystem, fileNamingScheme)
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
		Assume.assumeTrue(fileNamingScheme.hasDiffsSupport());

		manager.saveDiff(100, 10L, 25, 1L);

		int integer = manager.loadDiff(25, 1L, 10L);
		assertEquals(100, integer);
	}

	@Test
	public void uploadsAreAtomic() throws IOException {
		IOException expectedException = new IOException("Failed");
		manager = FileStateManager.<Long, Integer>builder(fileSystem, fileNamingScheme)
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

		IOException e = assertThrows(IOException.class, () -> manager.save(125));
		assertSame(expectedException, e);
		assertEquals(2, fileSystem.list("**").size()); // no new files are created
	}

	@Test
	public void saveArbitraryRevision() throws IOException {
		manager.save(100, 10L);
		manager.save(200, 20L);
		manager.save(150, 30L);

		assertThrows(IllegalArgumentException.class, () -> manager.save(500, 25L));

		//noinspection DataFlowIssue
		long lastSnapshotRevision = manager.getLastSnapshotRevision();
		Integer loaded = manager.loadSnapshot(lastSnapshotRevision);
		assertEquals(150, loaded.intValue());
		assertEquals(30L, lastSnapshotRevision);

		assertEquals(100, manager.loadSnapshot(10L).intValue());
	}

	@Test
	public void saveAndLoadFrom() throws IOException {
		manager.saveSnapshot(123, 1L);
		manager.saveSnapshot(345, 2L);
		manager.saveSnapshot(-3245, 3L);

		//noinspection DataFlowIssue
		long lastSnapshotRevision = manager.getLastSnapshotRevision();
		Integer loaded = manager.loadSnapshot(lastSnapshotRevision);
		assertEquals(-3245, loaded.intValue());
		assertEquals(3L, lastSnapshotRevision);
	}

	@Test
	public void cleanup() throws IOException {
		int maxSaveDiffs = 3;
		manager = FileStateManager.<Long, Integer>builder(fileSystem, fileNamingScheme)
			.withCodec(new IntegerCodec())
			.withMaxSaveDiffs(maxSaveDiffs)
			.build();

		manager.save(100);  // 1
		manager.save(200);  // 2
		manager.save(300);  // 3
		manager.save(400);  // 4
		manager.save(500);  // 5

		Long lastSnapshotRevision = manager.getLastSnapshotRevision();
		assertNotNull(lastSnapshotRevision);
		assertEquals(5, (long) lastSnapshotRevision);

		assertEquals(300, manager.loadSnapshot(3L).intValue());

		if (fileNamingScheme.hasDiffsSupport()) {
			Long lastDiffRevision = manager.getLastDiffRevision(3L);
			assertNotNull(lastDiffRevision);
			assertEquals(5, lastDiffRevision.longValue());
			System.out.println(lastDiffRevision);
		}

		manager.cleanup(2);

		assertThrows(IOException.class, () -> manager.loadSnapshot(3L));
		if (fileNamingScheme.hasDiffsSupport()) {
			assertNull(manager.getLastDiffRevision(3L));
		}
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
