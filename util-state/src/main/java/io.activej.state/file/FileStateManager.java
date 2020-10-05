package io.activej.state.file;

import io.activej.fs.BlockingFs;
import io.activej.fs.FileMetadata;
import io.activej.serializer.datastream.*;
import io.activej.state.StateManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;

import static io.activej.common.Checks.checkArgument;

@SuppressWarnings({"unused", "unchecked"})
public final class FileStateManager<T> implements StateManager<T, Long> {
	public static final String DEFAULT_TEMP_DIR = ".temp/";

	private final BlockingFs fs;
	private final FileNamingScheme fileNamingScheme;
	private final DataStreamEncoder<T> encoder;
	private final DataStreamDecoder<T> decoder;

	private int maxSaveDiffs = 0;
	private String tempDir = DEFAULT_TEMP_DIR;

	private FileStateManager(BlockingFs fs, FileNamingScheme fileNamingScheme,
			DataStreamEncoder<T> encoder, DataStreamDecoder<T> decoder) {
		this.fs = fs;
		this.fileNamingScheme = fileNamingScheme;
		this.encoder = encoder;
		this.decoder = decoder;
	}

	public static <T> FileStateManager<T> create(BlockingFs fs, FileNamingScheme fileNamingScheme,
			DataStreamCodec<T> codec) {
		return new FileStateManager<>(fs, fileNamingScheme, codec, codec);
	}

	public static <T> FileStateManager<T> create(BlockingFs fs, FileNamingScheme fileNamingScheme,
			DataStreamEncoder<T> encoder, DataStreamDecoder<T> decoder) {
		return new FileStateManager<>(fs, fileNamingScheme, encoder, decoder);
	}

	public static <T> FileStateManager<T> create(BlockingFs fs, FileNamingScheme fileNamingScheme,
			DataStreamEncoder<T> encoder) {
		return new FileStateManager<>(fs, fileNamingScheme, encoder, null);
	}

	public static <T> FileStateManager<T> create(BlockingFs fs, FileNamingScheme fileNamingScheme,
			DataStreamDecoder<T> decoder) {
		return new FileStateManager<>(fs, fileNamingScheme, null, decoder);
	}

	public FileStateManager<T> withMaxSaveDiffs(int maxSaveDiffs) {
		this.maxSaveDiffs = maxSaveDiffs;
		return this;
	}

	public FileStateManager<T> withTempDir(String tempDir) {
		checkArgument(!tempDir.isEmpty() && !tempDir.equals("/"), "Temporary directory cannot be same as main directory");
		this.tempDir = tempDir.endsWith("/") ? tempDir : tempDir + '/';
		return this;
	}

	@Override
	public @NotNull Long newRevision() throws IOException {
		Long lastSnapshotRevision = getLastSnapshotRevision();
		return lastSnapshotRevision == null ? 1L : lastSnapshotRevision + 1;
	}

	@Override
	public @Nullable Long getLastSnapshotRevision() throws IOException {
		Map<String, FileMetadata> list = fs.list(fileNamingScheme.snapshotGlob());
		OptionalLong max = list.keySet().stream().map(fileNamingScheme::decodeSnapshot).filter(Objects::nonNull).mapToLong(v -> v).max();
		return max.isPresent() ? max.getAsLong() : null;
	}

	@Override
	public @Nullable Long getLastDiffRevision(@NotNull Long currentRevision) throws IOException {
		Map<String, FileMetadata> list = fs.list(fileNamingScheme.diffGlob(currentRevision));
		OptionalLong max = list.keySet().stream().map(fileNamingScheme::decodeDiff).filter(Objects::nonNull).mapToLong(FileNamingScheme.Diff::getTo).max();
		return max.isPresent() ? max.getAsLong() : null;
	}

	@Override
	public @NotNull T loadSnapshot(@NotNull Long revision) throws IOException {
		String filename = fileNamingScheme.encodeSnapshot(revision);
		InputStream inputStream = fs.download(filename);
		try (DataInputStreamEx inputStreamEx = DataInputStreamEx.create(inputStream)) {
			return decoder.decode(inputStreamEx);
		}
	}

	@Override
	public @NotNull T loadDiff(@NotNull T state, @NotNull Long revisionFrom, @NotNull Long revisionTo) throws IOException {
		if (!(this.decoder instanceof DiffDataStreamDecoder)) {
			throw new UnsupportedOperationException();
		}
		String filename = fileNamingScheme.encodeDiff(revisionFrom, revisionTo);
		InputStream inputStream = fs.download(filename);
		try (DataInputStreamEx inputStreamEx = DataInputStreamEx.create(inputStream)) {
			return ((DiffDataStreamDecoder<T>) this.decoder).decodeDiff(inputStreamEx, state);
		}
	}

	@Override
	public void saveSnapshot(@NotNull T state, @NotNull Long revision) throws IOException {
		String filename = fileNamingScheme.encodeSnapshot(revision);
		safeUpload(filename, outputStream -> encoder.encode(outputStream, state));
	}

	@Override
	public void saveDiff(@NotNull T state, @NotNull Long revision, @NotNull T stateFrom, @NotNull Long revisionFrom) throws IOException {
		String filenameDiff = fileNamingScheme.encodeDiff(revisionFrom, revision);
		safeUpload(filenameDiff, outputStream -> ((DiffDataStreamEncoder<T>) this.encoder).encodeDiff(outputStream, stateFrom, state));
	}

	public @NotNull FileState<T> load() throws IOException {
		Long lastRevision = getLastSnapshotRevision();
		if (lastRevision != null) {
			return new FileState<>(loadSnapshot(lastRevision), lastRevision);
		}
		throw new IOException();
	}

	public @NotNull FileState<T> load(@NotNull T stateFrom, @NotNull Long revisionFrom) throws IOException {
		Long lastRevision = getLastSnapshotRevision();
		if (decoder instanceof DiffDataStreamDecoder) {
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
		throw new IOException();
	}

	public @NotNull Long save(@NotNull T state) throws IOException {
		long revision = newRevision();

		if (maxSaveDiffs != 0) {
			Map<String, FileMetadata> list = fs.list(fileNamingScheme.snapshotGlob());
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
				InputStream inputStream = fs.download(filenameFrom);
				T stateFrom;
				try (DataInputStreamEx inputStreamEx = DataInputStreamEx.create(inputStream)) {
					stateFrom = decoder.decode(inputStreamEx);
				}

				String filenameDiff = fileNamingScheme.encodeDiff(revisionFrom, revision);
				safeUpload(filenameDiff, outputStream -> ((DiffDataStreamEncoder<T>) this.encoder).encodeDiff(outputStream, state, stateFrom));
			}
		}

		String filename = fileNamingScheme.encodeSnapshot(revision);
		safeUpload(filename, outputStream -> encoder.encode(outputStream, state));

		return revision;
	}

	private void safeUpload(String filename, DataOutputStreamExConsumer consumer) throws IOException {
		String tempFilename = tempDir + UUID.randomUUID().toString();
		OutputStream outputStream = fs.upload(tempFilename);
		try (DataOutputStreamEx outputStreamEx = DataOutputStreamEx.create(outputStream)) {
			consumer.accept(outputStreamEx);
		} catch (IOException e) {
			try {
				fs.delete(tempFilename);
			} catch (IOException e1) {
				e.addSuppressed(e1);
			}
			throw e;
		}

		fs.move(tempFilename, filename);
	}

	private interface DataOutputStreamExConsumer {
		void accept(DataOutputStreamEx outputStream) throws IOException;
	}

}
