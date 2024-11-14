package io.activej.fs.json;

import io.activej.fs.FileMetadata;
import io.activej.fs.exception.*;
import io.activej.fs.http.UploadAcknowledgement;
import io.activej.json.JsonCodec;
import io.activej.json.JsonConstructor2;
import io.activej.json.JsonValidationException;
import io.activej.json.SubclassJsonCodec;

import static io.activej.json.JsonCodecs.*;

public class JsonCodecs {

	public static JsonCodec<FileSystemException> ofFileSystemException() {
		return SubclassJsonCodec.<FileSystemException>builder()
			.with(FileSystemException.class, simpleCodec(FileSystemException::new))
			.with(FileSystemIOException.class, simpleCodec(FileSystemIOException::new))
			.with(FileSystemStateException.class, simpleCodec(FileSystemStateException::new))
			.with(FileSystemScalarException.class, ofFileSystemScalarException())
			.with(FileSystemBatchException.class, ofObject(exceptions -> new FileSystemBatchException(exceptions, false),
				"exceptions", FileSystemBatchException::getExceptions, ofMap(ofFileSystemScalarException())))
			.build();
	}

	public static JsonCodec<FileSystemScalarException> ofFileSystemScalarException() {
		return SubclassJsonCodec.<FileSystemScalarException>builder()
			.with(FileSystemScalarException.class, simpleCodec(FileSystemScalarException::new))
			.with(MalformedGlobException.class, simpleCodec(MalformedGlobException::new))
			.with(ForbiddenPathException.class, simpleCodec(ForbiddenPathException::new))
			.with(IllegalOffsetException.class, simpleCodec(IllegalOffsetException::new))
			.with(IsADirectoryException.class, simpleCodec(IsADirectoryException::new))
			.with(PathContainsFileException.class, simpleCodec(PathContainsFileException::new))
			.with(FileNotFoundException.class, simpleCodec(FileNotFoundException::new))
			.build();
	}

	private static <T extends FileSystemException> JsonCodec<T> simpleCodec(JsonConstructor2<String, Boolean, T> constructor) {
		return ofObject(message -> constructor.create(message, false),
			"message", FileSystemException::getMessage, ofString());
	}

	public static JsonCodec<FileMetadata> ofFileMetadata() {
		return ofObject(
			JsonCodecs::decodeFileMetadata, "size", FileMetadata::getSize, ofLong(),
			"timestamp", FileMetadata::getTimestamp, ofLong()
		);
	}

	public static JsonCodec<UploadAcknowledgement> ofUploadAcknowledgement() {
		return ofObject(UploadAcknowledgement::new,
			"error", UploadAcknowledgement::getError, ofFileSystemException().nullable());
	}

	public static FileMetadata decodeFileMetadata(long size, long timestamp) throws JsonValidationException {
		if (size < 0) {
			throw new JsonValidationException("Size is less than zero");
		}
		return FileMetadata.of(size, timestamp);
	}
}
