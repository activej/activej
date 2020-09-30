package io.activej.state;

import io.activej.fs.LocalBlockingFs;
import io.activej.serializer.SerializeException;
import io.activej.serializer.datastream.DataInputStreamEx;
import io.activej.serializer.datastream.DataOutputStreamEx;
import io.activej.serializer.datastream.DeserializeException;
import io.activej.serializer.datastream.DiffDataStreamCodec;
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

import static org.junit.Assert.assertEquals;

public class FileStateManagerTest {

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private FileStateManager<Integer> manager;

	@Before
	public void setUp() throws Exception {
		Path storage = tmpFolder.newFolder().toPath();
		LocalBlockingFs fs = LocalBlockingFs.create(storage);
		fs.start();

		FileNamingScheme namingScheme = FileNamingSchemes.create("", "", "", "", '-');
		manager = FileStateManager.create(fs, namingScheme, new IntegerCodec());
	}

	@Test
	public void saveAndLoad() throws IOException, DeserializeException, SerializeException {
		long revision = manager.save(100);
		FileState<Integer> loaded = manager.load();

		assertEquals(100, (int) loaded.getState());
		assertEquals(revision, loaded.getRevision());
	}

	@Test
	public void saveAndLoadWithRevisions() throws IOException, DeserializeException, SerializeException {
		manager.withMaxSaveDiffs(3);

		manager.save(100);
		manager.save(101);
		manager.save(110);
		manager.save(150);
		long lastRevision = manager.save(300);

		FileState<Integer> loaded = manager.load();

		assertEquals(300, (int) loaded.getState());
		assertEquals(lastRevision, loaded.getRevision());
	}

	@Test
	public void newRevision() throws IOException, SerializeException {
		assertEquals(1, (long) manager.newRevision());
		long newRevision = manager.newRevision();

		assertEquals(1, newRevision);
		manager.saveSnapshot(123, newRevision);

		assertEquals(2, (long) manager.newRevision());
	}

	@Test
	public void getLastSnapshotRevision() throws IOException, SerializeException {
		manager.saveSnapshot(12, 3L);
		manager.saveSnapshot(13, 123L);
		manager.saveSnapshot(11, 12L);
		manager.saveSnapshot(16, 56L);

		assertEquals(Long.valueOf(123), manager.getLastSnapshotRevision());
	}

	@Test
	public void getLastDiffRevision() throws SerializeException, DeserializeException, IOException {
		int maxSaveDiffs = 3;
		manager.withMaxSaveDiffs(maxSaveDiffs);

		Long revision = manager.save(100);
		manager.save(200);
		manager.save(300);
		manager.save(400);
		manager.save(500);

		Long lastDiffRevision = manager.getLastDiffRevision(revision);
		assertEquals(Long.valueOf(revision + maxSaveDiffs), lastDiffRevision);
	}

	@Test
	public void saveAndLoadSnapshot() throws IOException, SerializeException, DeserializeException {
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

	private static class IntegerCodec implements DiffDataStreamCodec<Integer> {
		@Override
		public Integer decode(DataInputStreamEx stream) throws IOException {
			return stream.readInt();
		}

		@Override
		public void encode(DataOutputStreamEx stream, Integer item) throws IOException {
			stream.writeInt(item);
		}

		@Override
		public Integer decodeDiff(DataInputStreamEx stream, Integer from) throws IOException {
			return from + stream.readInt();
		}

		@Override
		public void encodeDiff(DataOutputStreamEx stream, Integer from, Integer to) throws IOException {
			stream.writeInt(to - from);
		}
	}
}
