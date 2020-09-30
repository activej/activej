package io.activej.state.file;

import io.activej.fs.BlockingFs;
import io.activej.fs.FileMetadata;
import io.activej.serializer.SerializeException;
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

@SuppressWarnings({"unused", "unchecked"})
public final class FileStateManager<T> implements StateManager<T, Long> {
	private final BlockingFs fs;
	private final FileNamingScheme fileNamingScheme;
	private final DataStreamEncoder<T> encoder;
	private final DataStreamDecoder<T> decoder;
	private int maxSaveDiffs = 0;

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
	public @NotNull T loadSnapshot(@NotNull Long revision) throws IOException, DeserializeException {
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
	public void saveSnapshot(@NotNull T state, @NotNull Long revision) throws IOException, SerializeException {
		String filename = fileNamingScheme.encodeSnapshot(revision);
		OutputStream outputStream = fs.upload(filename);
		try (DataOutputStreamEx outputStreamEx = DataOutputStreamEx.create(outputStream)) {
			encoder.encode(outputStreamEx, state);
		}
	}

	@Override
	public void saveDiff(@NotNull T state, @NotNull Long revision, @NotNull T stateFrom, @NotNull Long revisionFrom) throws IOException {
		String filenameDiff = fileNamingScheme.encodeDiff(revisionFrom, revision);
		OutputStream outputStreamDiff = fs.upload(filenameDiff);
		try (DataOutputStreamEx outputStreamExDiff = DataOutputStreamEx.create(outputStreamDiff)) {
			((DiffDataStreamEncoder<T>) this.encoder).encodeDiff(outputStreamExDiff, stateFrom, state);
		}
	}

	public @NotNull FileState<T> load() throws IOException, DeserializeException {
		Long lastRevision = getLastSnapshotRevision();
		if (lastRevision != null) {
			return new FileState<>(loadSnapshot(lastRevision), lastRevision);
		}
		throw new IOException();
	}

	public @NotNull FileState<T> load(@NotNull T stateFrom, @NotNull Long revisionFrom) throws IOException, DeserializeException {
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

	public @NotNull Long save(@NotNull T state) throws IOException, SerializeException, DeserializeException {
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
				OutputStream outputStreamDiff = fs.upload(filenameDiff);
				try (DataOutputStreamEx outputStreamExDiff = DataOutputStreamEx.create(outputStreamDiff)) {
					((DiffDataStreamEncoder<T>) this.encoder).encodeDiff(outputStreamExDiff, state, stateFrom);
				}
			}
		}

		String filename = fileNamingScheme.encodeSnapshot(revision);
		OutputStream outputStream = fs.upload(filename);
		try (DataOutputStreamEx outputStreamEx = DataOutputStreamEx.create(outputStream)) {
			encoder.encode(outputStreamEx, state);
		}

		return revision;
	}

}
