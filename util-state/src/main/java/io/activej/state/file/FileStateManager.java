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
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;

@SuppressWarnings({"unused"})
public final class FileStateManager<R extends Comparable<R>, T> implements IStateManager<R, T> {
	public static final String DEFAULT_TEMP_DIR = ".temp/";

	private final IBlockingFileSystem fileSystem;
	private final FileNamingScheme<R> fileNamingScheme;
	private Supplier<? extends StreamEncoder<T>> encoderSupplier;
	private Supplier<? extends StreamDecoder<T>> decoderSupplier;

	private @Nullable InputStreamWrapper inputStreamWrapper;
	private @Nullable OutputStreamWrapper outputStreamWrapper;

	private int maxSaveDiffs = 0;
	private String tempDir = DEFAULT_TEMP_DIR;

	private int maxRevisions;

	private FileStateManager(IBlockingFileSystem fileSystem, FileNamingScheme<R> fileNamingScheme) {
		this.fileSystem = fileSystem;
		this.fileNamingScheme = fileNamingScheme;
	}

	public static <R extends Comparable<R>, T> FileStateManager<R, T>.Builder builder(IBlockingFileSystem fileSystem, FileNamingScheme<R> fileNamingScheme) {
		return new FileStateManager<R, T>(fileSystem, fileNamingScheme).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, FileStateManager<R, T>> {
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
		protected FileStateManager<R, T> doBuild() {
			checkState(encoderSupplier != null || decoderSupplier != null, "Neither encoder nor decoder are set");
			return FileStateManager.this;
		}
	}

	public FileNamingScheme<R> getFileNamingScheme() {
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
	public boolean hasDiffsSupport() {
		return fileNamingScheme.hasDiffsSupport();
	}

	@Override
	public R newRevision() throws IOException {
		R lastSnapshotRevision = getLastSnapshotRevision();
		return fileNamingScheme.nextRevision(lastSnapshotRevision);
	}

	@Override
	public @Nullable R getLastSnapshotRevision() throws IOException {
		Pattern pattern = fileNamingScheme.snapshotGlob();
		Map<String, FileMetadata> list = fileSystem.list("*");
		R best = null;
		for (String s : list.keySet()) {
			if (!pattern.matcher(s).matches()) continue;
			R revision = fileNamingScheme.decodeSnapshot(s);
			if (best == null || revision.compareTo(best) > 0) {
				best = revision;
			}
		}
		return best;
	}

	@Override
	public @Nullable R getLastDiffRevision(R currentRevision) throws IOException {
		if (!hasDiffsSupport()) throw new UnsupportedOperationException();
		Pattern pattern = fileNamingScheme.diffGlob();
		Map<String, FileMetadata> list = fileSystem.list("*");
		R best = null;
		for (String s : list.keySet()) {
			if (!pattern.matcher(s).matches()) continue;
			var diff = fileNamingScheme.decodeDiff(s);
			if (!diff.from().equals(currentRevision)) continue;
			if (best == null || diff.to().compareTo(best) > 0) {
				best = diff.to();
			}
		}
		return best;
	}

	@Override
	public T loadSnapshot(R revision) throws IOException {
		String filename = fileNamingScheme.encodeSnapshot(revision);
		if (fileSystem.info(filename) == null) {
			throw new IOException("Cannot find snapshot with revision " + revision);
		}

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
	public T loadDiff(T state, R revisionFrom, R revisionTo) throws IOException {
		if (!hasDiffsSupport()) throw new UnsupportedOperationException();
		if (revisionFrom.equals(revisionTo)) return state;
		String filename = fileNamingScheme.encodeDiff(revisionFrom, revisionTo);
		if (fileSystem.info(filename) == null) {
			throw new IOException("Cannot find diffs between revision " + revisionFrom + " and " + revisionTo);
		}

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
	public void saveSnapshot(T state, R revision) throws IOException {
		String filename = fileNamingScheme.encodeSnapshot(revision);
		StreamEncoder<T> encoder = encoderSupplier.get();
		safeUpload(filename, output -> encoder.encode(output, state));
	}

	@Override
	public void saveDiff(T state, R revision, T stateFrom, R revisionFrom) throws IOException {
		if (!hasDiffsSupport()) throw new UnsupportedOperationException();
		String filenameDiff = fileNamingScheme.encodeDiff(revisionFrom, revision);
		DiffStreamEncoder<T> encoder = (DiffStreamEncoder<T>) encoderSupplier.get();
		safeUpload(filenameDiff, output -> encoder.encodeDiff(output, stateFrom, state));
	}

	public R save(T state) throws IOException {
		R revision = newRevision();
		doSave(state, revision);
		return revision;
	}

	public void save(T state, R revision) throws IOException {
		R lastRevision = getLastSnapshotRevision();
		if (lastRevision != null && lastRevision.compareTo(revision) >= 0) {
			throw new IllegalArgumentException("Revision cannot be less than last revision [" + lastRevision + ']');
		}
		doSave(state, revision);
	}

	private void doSave(T state, R revision) throws IOException {
		if (maxSaveDiffs != 0) {
			Pattern pattern = fileNamingScheme.snapshotGlob();
			Map<String, FileMetadata> filenames = fileSystem.list("*");
			PriorityQueue<R> top = new PriorityQueue<>(maxSaveDiffs);
			for (var entry : filenames.entrySet()) {
				if (!pattern.matcher(entry.getKey()).matches()) continue;
				R snapshotRevision = fileNamingScheme.decodeSnapshot(entry.getKey());
				top.add(snapshotRevision);
				if (top.size() > maxSaveDiffs) top.poll();
			}

			while (!top.isEmpty()) {
				R revisionFrom = top.poll();
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

		PriorityQueue<R> top = new PriorityQueue<>(maxRevisions);
		for (var entry : filenames.entrySet()) {
			R snapshotRevision = fileNamingScheme.decodeSnapshot(entry.getKey());
			top.add(snapshotRevision);
			if (top.size() > maxSaveDiffs) top.poll();
		}

		if (top.isEmpty()) return;
		R minRetainedRevision = top.poll();
		for (String filename : filenames.keySet()) {
			FileNamingScheme.Diff<R> diff = fileNamingScheme.decodeDiff(filename);
			if (diff != null && diff.from().compareTo(minRetainedRevision) < 0) {
				fileSystem.delete(filename);
			}
		}
		for (String filename : filenames.keySet()) {
			R snapshot = fileNamingScheme.decodeSnapshot(filename);
			if (snapshot != null && snapshot.compareTo(minRetainedRevision) < 0) {
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
