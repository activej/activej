package io.activej.state.file;

import io.activej.common.builder.AbstractBuilder;
import io.activej.fs.FileMetadata;
import io.activej.fs.IBlockingFileSystem;
import io.activej.serializer.stream.*;
import io.activej.state.IStateManager;
import io.activej.state.file.FileNamingDiffScheme.Diff;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.Supplier;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static java.util.Comparator.reverseOrder;

@SuppressWarnings({"unused"})
public final class FileStateManager<T> implements IStateManager<T, Long> {
	public static final String DEFAULT_TEMP_DIR = ".temp/";

	private final IBlockingFileSystem fileSystem;
	private final FileNamingScheme fileNamingScheme;
	private Supplier<? extends StreamEncoder<T>> encoderSupplier;
	private Supplier<? extends StreamDecoder<T>> decoderSupplier;

	private @Nullable InputStreamWrapper inputStreamWrapper;
	private @Nullable OutputStreamWrapper outputStreamWrapper;

	private int maxSaveDiffs = 0;
	private String tempDir = DEFAULT_TEMP_DIR;

	private int maxRevisions;

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
			FileStateManager.this.encoderSupplier = () -> encoder;
			return this;
		}

		public Builder withEncoder(Supplier<? extends StreamEncoder<T>> encoderSupplier) {
			checkNotBuilt(this);
			FileStateManager.this.encoderSupplier = encoderSupplier;
			return this;
		}

		public Builder withDecoder(StreamDecoder<T> decoder) {
			checkNotBuilt(this);
			FileStateManager.this.decoderSupplier = () -> decoder;
			return this;
		}

		public Builder withDecoder(Supplier<? extends StreamDecoder<T>> decoderSupplier) {
			checkNotBuilt(this);
			FileStateManager.this.decoderSupplier = decoderSupplier;
			return this;
		}

		public Builder withCodec(StreamCodec<T> codec) {
			checkNotBuilt(this);
			FileStateManager.this.encoderSupplier = () -> codec;
			FileStateManager.this.decoderSupplier = () -> codec;
			return this;
		}

		public Builder withCodec(Supplier<? extends StreamCodec<T>> codecSupplier) {
			checkNotBuilt(this);
			FileStateManager.this.encoderSupplier = codecSupplier;
			FileStateManager.this.decoderSupplier = codecSupplier;
			return this;
		}

		public Builder withMaxSaveDiffs(int maxSaveDiffs) {
			checkNotBuilt(this);
			checkArgument(maxSaveDiffs >= 0);
			FileStateManager.this.maxSaveDiffs = maxSaveDiffs;
			return this;
		}

		public Builder withMaxRevisions(int maxRevisions) {
			checkNotBuilt(this);
			checkArgument(maxRevisions >= 0);
			FileStateManager.this.maxRevisions = maxRevisions;
			return this;
		}

		public Builder withTempDir(String tempDir) {
			checkNotBuilt(this);
			checkArgument(!tempDir.isEmpty() && !tempDir.equals("/"), "Temporary directory cannot be same as main directory");
			FileStateManager.this.tempDir = tempDir.endsWith("/") ? tempDir : tempDir + '/';
			return this;
		}

		public Builder withDownloadWrapper(InputStreamWrapper wrapper) {
			checkNotBuilt(this);
			FileStateManager.this.inputStreamWrapper = wrapper;
			return this;
		}

		public Builder withUploadWrapper(OutputStreamWrapper wrapper) {
			checkNotBuilt(this);
			FileStateManager.this.outputStreamWrapper = wrapper;
			return this;
		}

		@Override
		protected FileStateManager<T> doBuild() {
			checkState(encoderSupplier != null || decoderSupplier != null, "Neither encoder nor decoder are set");
			return FileStateManager.this;
		}
	}

	public FileNamingScheme getFileNamingScheme() {
		return fileNamingScheme;
	}

	public IBlockingFileSystem getFileSystem() {
		return fileSystem;
	}

	public StreamEncoder<T> getEncoder() {
		return encoderSupplier.get();
	}

	public StreamDecoder<T> getDecoder() {
		return decoderSupplier.get();
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
		if (!(fileNamingScheme instanceof FileNamingDiffScheme)) throw new UnsupportedOperationException();
		FileNamingDiffScheme fileNamingScheme = (FileNamingDiffScheme) this.fileNamingScheme;
		Map<String, FileMetadata> list = fileSystem.list(fileNamingScheme.diffGlob(currentRevision));
		OptionalLong max = list.keySet().stream().map(fileNamingScheme::decodeDiff).filter(Objects::nonNull).mapToLong(Diff::to).max();
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
		if (inputStreamWrapper != null) {
			inputStream = inputStreamWrapper.wrap(inputStream);
		}
		try (StreamInput input = StreamInput.create(inputStream)) {
			StreamDecoder<T> decoder = decoderSupplier.get();
			return decoder.decode(input);
		}
	}

	@Override
	public T loadDiff(T state, Long revisionFrom, Long revisionTo) throws IOException {
		if (!(fileNamingScheme instanceof FileNamingDiffScheme)) throw new UnsupportedOperationException();
		T loaded = tryLoadDiff(state, revisionFrom, revisionTo);
		if (loaded == null) {
			throw new IOException("Cannot find diffs between revision " + revisionFrom + " and " + revisionTo);
		}
		return loaded;
	}

	@Override
	public @Nullable T tryLoadDiff(T state, Long revisionFrom, Long revisionTo) throws IOException {
		if (!(fileNamingScheme instanceof FileNamingDiffScheme)) throw new UnsupportedOperationException();
		FileNamingDiffScheme fileNamingScheme = (FileNamingDiffScheme) this.fileNamingScheme;
		if (revisionFrom.equals(revisionTo)) return state;
		String filename = fileNamingScheme.encodeDiff(revisionFrom, revisionTo);
		if (fileSystem.info(filename) == null) return null;

		InputStream inputStream = fileSystem.download(filename);
		if (inputStreamWrapper != null) {
			inputStream = inputStreamWrapper.wrap(inputStream);
		}
		try (StreamInput input = StreamInput.create(inputStream)) {
			DiffStreamDecoder<T> decoder = (DiffStreamDecoder<T>) this.decoderSupplier.get();
			return decoder.decodeDiff(input, state);
		}
	}

	@Override
	public void saveSnapshot(T state, Long revision) throws IOException {
		String filename = fileNamingScheme.encodeSnapshot(revision);
		StreamEncoder<T> encoder = encoderSupplier.get();
		safeUpload(filename, output -> encoder.encode(output, state));
	}

	@Override
	public void saveDiff(T state, Long revision, T stateFrom, Long revisionFrom) throws IOException {
		if (!(fileNamingScheme instanceof FileNamingDiffScheme)) throw new UnsupportedOperationException();
		FileNamingDiffScheme fileNamingScheme = (FileNamingDiffScheme) this.fileNamingScheme;
		String filenameDiff = fileNamingScheme.encodeDiff(revisionFrom, revision);
		DiffStreamEncoder<T> encoder = (DiffStreamEncoder<T>) encoderSupplier.get();
		safeUpload(filenameDiff, output -> encoder.encodeDiff(output, stateFrom, state));
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
		if (fileNamingScheme instanceof FileNamingDiffScheme) {
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
			FileNamingDiffScheme fileNamingScheme = (FileNamingDiffScheme) this.fileNamingScheme;

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
				if (inputStreamWrapper != null) {
					inputStream = inputStreamWrapper.wrap(inputStream);
				}
				T stateFrom;
				try (StreamInput input = StreamInput.create(inputStream)) {
					StreamDecoder<T> decoder = decoderSupplier.get();
					stateFrom = decoder.decode(input);
				}

				String filenameDiff = fileNamingScheme.encodeDiff(revisionFrom, revision);
				DiffStreamEncoder<T> encoder = (DiffStreamEncoder<T>) encoderSupplier.get();
				safeUpload(filenameDiff, output -> encoder.encodeDiff(output, state, stateFrom));
			}
		}

		String filename = fileNamingScheme.encodeSnapshot(revision);
		StreamEncoder<T> encoder = encoderSupplier.get();
		safeUpload(filename, output -> encoder.encode(output, state));
	}

	private void safeUpload(String filename, StreamOutputConsumer consumer) throws IOException {
		String tempFilename = tempDir + UUID.randomUUID();
		OutputStream outputStream = fileSystem.upload(tempFilename);
		if (outputStreamWrapper != null) {
			outputStream = outputStreamWrapper.wrap(outputStream);
		}
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

		if (maxRevisions != 0) {
			try {
				cleanup(maxRevisions);
			} catch (IOException ignored) {
			}
		}
	}

	public void cleanup(int maxRevisions) throws IOException {
		Map<String, FileMetadata> filenames = fileSystem.list("**");
		var retainedMinRevisionOpt = filenames.keySet().stream()
			.map(fileNamingScheme::decodeSnapshot)
			.filter(Objects::nonNull)
			.sorted(reverseOrder())
			.limit(maxRevisions)
			.sorted()
			.findFirst();
		if (retainedMinRevisionOpt.isEmpty()) return;
		long minRetainedRevision = retainedMinRevisionOpt.get();
		if (fileNamingScheme instanceof FileNamingDiffScheme) {
			FileNamingDiffScheme fileNamingScheme = (FileNamingDiffScheme) this.fileNamingScheme;
			for (String filename : filenames.keySet()) {
				Diff diff = fileNamingScheme.decodeDiff(filename);
				if (diff != null && diff.from() < minRetainedRevision) {
					fileSystem.delete(filename);
				}
			}
		}
		for (String filename : filenames.keySet()) {
			Long snapshot = fileNamingScheme.decodeSnapshot(filename);
			if (snapshot != null && snapshot < minRetainedRevision) {
				fileSystem.delete(filename);
			}
		}
	}

	public interface InputStreamWrapper {
		InputStream wrap(InputStream inputStream) throws IOException;
	}

	public interface OutputStreamWrapper {
		OutputStream wrap(OutputStream inputStream) throws IOException;
	}

	private interface StreamOutputConsumer {
		void accept(StreamOutput outputStream) throws IOException;
	}
}
