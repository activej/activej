package io.activej.fs.exception;

import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.serializer.stream.StreamCodecs.SubtypeBuilder;

import java.util.function.BiFunction;

public final class FileSystemExceptionStreamCodec {
	public static StreamCodec<FileSystemException> createFileSystemExceptionCodec() {
		SubtypeBuilder<FileSystemException> builder = new SubtypeBuilder<>();

		StreamCodec<FileSystemScalarException> scalarExceptionCodec = simpleCodec(FileSystemScalarException::new);

		builder.add(FileSystemBatchException.class, StreamCodec.create(exceptions -> new FileSystemBatchException(exceptions, false),
			FileSystemBatchException::getExceptions, StreamCodecs.ofMap(StreamCodecs.ofString(), scalarExceptionCodec))
		);
		builder.add(FileSystemException.class, simpleCodec(FileSystemException::new));
		builder.add(FileSystemStateException.class, simpleCodec(FileSystemStateException::new));
		builder.add(FileSystemScalarException.class, scalarExceptionCodec);
		builder.add(PathContainsFileException.class, simpleCodec(PathContainsFileException::new));
		builder.add(IllegalOffsetException.class, simpleCodec(IllegalOffsetException::new));
		builder.add(FileNotFoundException.class, simpleCodec(FileNotFoundException::new));
		builder.add(ForbiddenPathException.class, simpleCodec(ForbiddenPathException::new));
		builder.add(MalformedGlobException.class, simpleCodec(MalformedGlobException::new));
		builder.add(IsADirectoryException.class, simpleCodec(IsADirectoryException::new));
		builder.add(FileSystemIOException.class, simpleCodec(FileSystemIOException::new));

		return builder.build();
	}

	private static <E extends FileSystemException> StreamCodec<E> simpleCodec(BiFunction<String, Boolean, E> constructor) {
		return StreamCodec.create(message -> constructor.apply(message, false),
			E::getMessage, StreamCodecs.ofString()
		);
	}
}
