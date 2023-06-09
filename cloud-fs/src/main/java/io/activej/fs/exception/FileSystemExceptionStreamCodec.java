package io.activej.fs.exception;

import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.serializer.stream.StreamCodecs.SubtypeStreamCodec;

import java.util.function.BiFunction;

public final class FileSystemExceptionStreamCodec {
	public static StreamCodec<FileSystemException> createFileSystemExceptionCodec() {
		StreamCodec<FileSystemScalarException> scalarExceptionCodec = simpleCodec(FileSystemScalarException::new);

		return SubtypeStreamCodec.<FileSystemException>builder()
			.withSubtype(FileSystemBatchException.class, StreamCodec.create(exceptions -> new FileSystemBatchException(exceptions, false),
				FileSystemBatchException::getExceptions, StreamCodecs.ofMap(StreamCodecs.ofString(), scalarExceptionCodec)))
			.withSubtype(FileSystemException.class, simpleCodec(FileSystemException::new))
			.withSubtype(FileSystemStateException.class, simpleCodec(FileSystemStateException::new))
			.withSubtype(FileSystemScalarException.class, scalarExceptionCodec)
			.withSubtype(PathContainsFileException.class, simpleCodec(PathContainsFileException::new))
			.withSubtype(IllegalOffsetException.class, simpleCodec(IllegalOffsetException::new))
			.withSubtype(FileNotFoundException.class, simpleCodec(FileNotFoundException::new))
			.withSubtype(ForbiddenPathException.class, simpleCodec(ForbiddenPathException::new))
			.withSubtype(MalformedGlobException.class, simpleCodec(MalformedGlobException::new))
			.withSubtype(IsADirectoryException.class, simpleCodec(IsADirectoryException::new))
			.withSubtype(FileSystemIOException.class, simpleCodec(FileSystemIOException::new))
			.build();
	}

	private static <E extends FileSystemException> StreamCodec<E> simpleCodec(BiFunction<String, Boolean, E> constructor) {
		return StreamCodec.create(message -> constructor.apply(message, false),
			E::getMessage, StreamCodecs.ofString()
		);
	}
}
