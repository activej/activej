package io.activej.state;

import io.activej.fs.BlockingFileSystem;
import io.activej.fs.FileMetadata;
import io.activej.serializer.stream.DiffStreamCodec;
import io.activej.serializer.stream.StreamInput;
import io.activej.serializer.stream.StreamOutput;
import io.activej.state.file.FileNamingScheme;
import io.activej.state.file.FileNamingSchemes;
import io.activej.state.file.FileStateManager;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.*;

public class FileStateManagerLZ4Test {

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	public static final FileNamingScheme<Long> NAMING_SCHEME = FileNamingSchemes.ofLong("", "", "", "", "-");

	private FileStateManager<Long, byte[]> manager;

	private BlockingFileSystem fileSystem;

	@Before
	public void setUp() throws Exception {
		Path storage = tmpFolder.newFolder().toPath();
		fileSystem = BlockingFileSystem.create(storage);
		fileSystem.start();

		manager = FileStateManager.<Long, byte[]>builder(fileSystem, NAMING_SCHEME)
			.withCodec(new BytesCodec())
			.withUploadWrapper(LZ4FrameOutputStream::new)
			.withDownloadWrapper(LZ4FrameInputStream::new)
			.build();
	}

	@Test
	public void saveAndLoad() throws IOException {
		byte[] state = new byte[BytesCodec.LENGTH];
		Arrays.fill(state, (byte) 10);

		long revision = manager.save(state);
		//noinspection DataFlowIssue
		long lastSnapshotRevision = manager.getLastSnapshotRevision();
		byte[] loaded = manager.loadSnapshot(lastSnapshotRevision);

		assertArrayEquals(state, loaded);
		assertEquals(revision, lastSnapshotRevision);

		Map<String, FileMetadata> list = fileSystem.list("**");
		assertEquals(1, list.size());

		long size = list.get("1").getSize();
		assertTrue(size < BytesCodec.LENGTH);
	}

	private static class BytesCodec implements DiffStreamCodec<byte[]> {
		private static final int LENGTH = 1_000_000;

		@Override
		public byte[] decode(StreamInput input) throws IOException {
			byte[] bytes = new byte[LENGTH];
			input.read(bytes);
			return bytes;
		}

		@Override
		public void encode(StreamOutput output, byte[] item) throws IOException {
			output.write(item);
		}

		@Override
		public byte[] decodeDiff(StreamInput input, byte[] from) throws IOException {
			byte[] diff = new byte[from.length];
			byte[] decoded = new byte[from.length];
			input.read(decoded);
			for (int i = 0; i < from.length; i++) {
				diff[i] = (byte) (from[i] + decoded[i]);
			}
			return diff;
		}

		@Override
		public void encodeDiff(StreamOutput output, byte[] from, byte[] to) throws IOException {
			byte[] diff = new byte[from.length];
			for (int i = 0; i < from.length; i++) {
				diff[i] = (byte) (to[i] - from[i]);
			}
			output.write(diff);
		}
	}

}
