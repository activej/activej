package io.activej.state.file;

import io.activej.common.builder.AbstractBuilder;
import io.activej.fs.FileMetadata;
import io.activej.fs.IBlockingFileSystem;
import io.activej.serializer.stream.*;
import io.activej.state.IStateManager;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;

@SuppressWarnings({"unused", "unchecked"})
public final class FileStateManager<T> implements IStateManager<T, Long> {
	public static final String DEFAULT_TEMP_DIR = ".temp/";

	private final IBlockingFileSystem fileSystem;
	private final FileNamingScheme fileNamingScheme;
	private StreamEncoder<T> encoder;
	private StreamDecoder<T> decoder;

	private int maxSaveDiffs = 0;
	private String tempDir = DEFAULT_TEMP_DIR;

	private FileStateManager(IBlockingFileSystem fileSystem, FileNamingScheme fileNamingScheme) {
		this.fileSystem = fileSystem;
		this.fileNamingScheme = fileNamingScheme;
	}

	public static <T> FileStateManager<T>.Builder builder(IBlockingFileSystem fileSystem, FileNamingScheme fileNamingScheme) {
		return new FileStateManager<T>(fileSystem, fileNamingScheme).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, FileStateManager<T>> {
		private Builder() {}

		public Builder withEncoder(StreamEncoder<T> encoder) {
			checkNotBuilt(this);
			FileStateManager.this.encoder = encoder;
			return this;
		}

		public Builder withDecoder(StreamDecoder<T> decoder) {
			checkNotBuilt(this);
			FileStateManager.this.decoder = decoder;
			return this;
		}

		public Builder withCodec(StreamCodec<T> codec) {
			checkNotBuilt(this);
			FileStateManager.this.encoder = codec;
			FileStateManager.this.decoder = codec;
			return this;
		}

		public Builder withMaxSaveDiffs(int maxSaveDiffs) {
			checkNotBuilt(this);
			checkArgument(maxSaveDiffs >= 0);
			FileStateManager.this.maxSaveDiffs = maxSaveDiffs;
			return this;
		}

		public Builder withTempDir(String tempDir) {
			checkNotBuilt(this);
			checkArgument(!tempDir.isEmpty() && !tempDir.equals("/"), "Temporary directory cannot be same as main directory");
			FileStateManager.this.tempDir = tempDir.endsWith("/") ? tempDir : tempDir + '/';
			return this;
		}

		@Override
		protected FileStateManager<T> doBuild() {
			checkState(encoder != null || decoder != null, "Neither encoder nor decoder are set");
			return FileStateManager.this;
		}
	}

	@Override
	public Long newRevision() throws IOException {
		Long lastSnapshotRevision = getLastSnapshotRevision();
		return lastSnapshotRevision == null ? 1L : lastSnapshotRevision + 1;
	}

	@Override
	public @Nullable Long getLastSnapshotRevision() throws IOException {
		Map<String, FileMetadata> list = fileSystem.list(fileNamingScheme.snapshotGlob());
		OptionalLong max = list.keySet().stream().map(fileNamingScheme::decodeSnapshot).filter(Objects::nonNull).mapToLong(v -> v).max();
		return max.isPresent() ? max.getAsLong() : null;
	}

	@Override
	public @Nullable Long getLastDiffRevision(Long currentRevision) throws IOException {
		Map<String, FileMetadata> list = fileSystem.list(fileNamingScheme.diffGlob(currentRevision));
		OptionalLong max = list.keySet().stream().map(fileNamingScheme::decodeDiff).filter(Objects::nonNull).mapToLong(FileNamingScheme.Diff::getTo).max();
		return max.isPresent() ? max.getAsLong() : null;
	}

	@Override
	public T loadSnapshot(Long revision) throws IOException {
		T loaded = tryLoadSnapshot(revision);
		if (loaded == null) {
			throw new IOException("Cannot find snapshot with revision " + revision);
		}
		return loaded;
	}

	@Override
	public @Nullable T tryLoadSnapshot(Long revision) throws IOException {
		String filename = fileNamingScheme.encodeSnapshot(revision);
		if (fileSystem.info(filename) == null) return null;

		InputStream inputStream = fileSystem.download(filename);
		try (StreamInput input = StreamInput.create(inputStream)) {
			return decoder.decode(input);
		}
	}

	@Override
	public T loadDiff(T state, Long revisionFrom, Long revisionTo) throws IOException {
		T loaded = tryLoadDiff(state, revisionFrom, revisionTo);
		if (loaded == null) {
			throw new IOException("Cannot find diffs between revision " + revisionFrom + " and " + revisionTo);
		}
		return loaded;
	}

	@Override
	public @Nullable T tryLoadDiff(T state, Long revisionFrom, Long revisionTo) throws IOException {
		if (revisionFrom.equals(revisionTo)) return state;
		if (!(this.decoder instanceof DiffStreamDecoder)) {
			throw new UnsupportedOperationException();
		}
		String filename = fileNamingScheme.encodeDiff(revisionFrom, revisionTo);
		if (fileSystem.info(filename) == null) return null;

		InputStream inputStream = fileSystem.download(filename);
		try (StreamInput input = StreamInput.create(inputStream)) {
			return ((DiffStreamDecoder<T>) this.decoder).decodeDiff(input, state);
		}
	}

	@Override
	public void saveSnapshot(T state, Long revision) throws IOException {
		String filename = fileNamingScheme.encodeSnapshot(revision);
		safeUpload(filename, output -> encoder.encode(output, state));
	}

	@Override
	public void saveDiff(T state, Long revision, T stateFrom, Long revisionFrom) throws IOException {
		String filenameDiff = fileNamingScheme.encodeDiff(revisionFrom, revision);
		safeUpload(filenameDiff, output -> ((DiffStreamEncoder<T>) this.encoder).encodeDiff(output, stateFrom, state));
	}

	public FileState<T> load() throws IOException {
		FileState<T> state = tryLoad();
		if (state == null) {
			throw new IOException("State is empty");
		}
		return state;
	}

	public @Nullable FileState<T> tryLoad() throws IOException {
		Long lastRevision = getLastSnapshotRevision();
		if (lastRevision != null) {
			return new FileState<>(loadSnapshot(lastRevision), lastRevision);
		}
		return null;
	}

	public FileState<T> load(T stateFrom, Long revisionFrom) throws IOException {
		FileState<T> state = tryLoad(stateFrom, revisionFrom);
		if (state == null) {
			throw new IOException("State is empty");
		}
		return state;
	}

	public @Nullable FileState<T> tryLoad(T stateFrom, Long revisionFrom) throws IOException {
		Long lastRevision = getLastSnapshotRevision();
		if (revisionFrom.equals(lastRevision)) {
			return new FileState<>(stateFrom, revisionFrom);
		}
		if (decoder instanceof DiffStreamDecoder) {
			Long lastDiffRevision = getLastDiffRevision(revisionFrom);
			if (lastDiffRevision != null && (lastRevision == null || lastDiffRevision.compareTo(lastRevision) >= 0)) {
				T state = loadDiff(stateFrom, revisionFrom, lastDiffRevision);
				return new FileState<>(state, lastDiffRevision);
			}
		}
		if (lastRevision != null) {
			T state = loadSnapshot(lastRevision);
			return new FileState<>(state, lastRevision);
		}
		return null;
	}

	public Long save(T state) throws IOException {
		long revision = newRevision();
		doSave(state, revision);
		return revision;
	}

	public void save(T state, Long revision) throws IOException {
		Long lastRevision = getLastSnapshotRevision();
		if (lastRevision != null && lastRevision >= revision) {
			throw new IllegalArgumentException("Revision cannot be less than last revision [" + lastRevision + ']');
		}
		doSave(state, revision);
	}

	private void doSave(T state, long revision) throws IOException {
		if (maxSaveDiffs != 0) {
			Map<String, FileMetadata> list = fileSystem.list(fileNamingScheme.snapshotGlob());
			long[] revisionsFrom = list.keySet().stream()
					.map(fileNamingScheme::decodeSnapshot)
					.filter(Objects::nonNull)
					.mapToLong(v -> v)
					.map(v -> -v)
					.sorted()
					.limit(maxSaveDiffs)
					.map(v -> -v)
					.toArray();

			for (long revisionFrom : revisionsFrom) {
				String filenameFrom = fileNamingScheme.encodeSnapshot(revisionFrom);
				InputStream inputStream = fileSystem.download(filenameFrom);
				T stateFrom;
				try (StreamInput input = StreamInput.create(inputStream)) {
					stateFrom = decoder.decode(input);
				}

				String filenameDiff = fileNamingScheme.encodeDiff(revisionFrom, revision);
				safeUpload(filenameDiff, output -> ((DiffStreamEncoder<T>) this.encoder).encodeDiff(output, state, stateFrom));
			}
		}

		String filename = fileNamingScheme.encodeSnapshot(revision);
		safeUpload(filename, output -> encoder.encode(output, state));
	}

	private void safeUpload(String filename, StreamOutputConsumer consumer) throws IOException {
		String tempFilename = tempDir + UUID.randomUUID();
		OutputStream outputStream = fileSystem.upload(tempFilename);
		try (StreamOutput outputStreamEx = StreamOutput.create(outputStream)) {
			consumer.accept(outputStreamEx);
		} catch (IOException e) {
			try {
				fileSystem.delete(tempFilename);
			} catch (IOException e1) {
				e.addSuppressed(e1);
			}
			throw e;
		}

		fileSystem.move(tempFilename, filename);
	}

	private interface StreamOutputConsumer {
		void accept(StreamOutput outputStream) throws IOException;
	}

}
