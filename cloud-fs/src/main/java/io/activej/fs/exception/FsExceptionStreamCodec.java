package io.activej.fs.exception;

import io.activej.streamcodecs.StreamCodec;
import io.activej.streamcodecs.StreamCodecs;
import io.activej.streamcodecs.StreamCodecs.SubtypeBuilder;
import io.activej.streamcodecs.StructuredStreamCodec;

import java.util.function.BiFunction;

public final class FsExceptionStreamCodec {
	public static StreamCodec<FsException> createFsExceptionCodec() {
		SubtypeBuilder<FsException> builder = new SubtypeBuilder<>();

		StreamCodec<FsScalarException> scalarExceptionCodec = simpleCodec(FsScalarException::new);

		builder.add(FsBatchException.class, StructuredStreamCodec.create(exceptions -> new FsBatchException(exceptions, false),
				FsBatchException::getExceptions, StreamCodecs.ofMap(StreamCodecs.ofString(), scalarExceptionCodec))
		);
		builder.add(FsException.class, simpleCodec(FsException::new));
		builder.add(FsStateException.class, simpleCodec(FsStateException::new));
		builder.add(FsScalarException.class, scalarExceptionCodec);
		builder.add(PathContainsFileException.class, simpleCodec(PathContainsFileException::new));
		builder.add(IllegalOffsetException.class, simpleCodec(IllegalOffsetException::new));
		builder.add(FileNotFoundException.class, simpleCodec(FileNotFoundException::new));
		builder.add(ForbiddenPathException.class, simpleCodec(ForbiddenPathException::new));
		builder.add(MalformedGlobException.class, simpleCodec(MalformedGlobException::new));
		builder.add(IsADirectoryException.class, simpleCodec(IsADirectoryException::new));
		builder.add(FsIOException.class, simpleCodec(FsIOException::new));

		return builder.build();
	}

	private static <E extends FsException> StreamCodec<E> simpleCodec(BiFunction<String, Boolean, E> constructor) {
		return StructuredStreamCodec.create(message -> constructor.apply(message, false),
				E::getMessage, StreamCodecs.ofString()
		);
	}
}
