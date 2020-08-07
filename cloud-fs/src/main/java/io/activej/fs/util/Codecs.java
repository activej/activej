/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.fs.util;

import io.activej.codec.CodecSubtype;
import io.activej.codec.StructuredCodec;
import io.activej.codec.StructuredCodecs;
import io.activej.common.tuple.TupleParser2;
import io.activej.fs.ActiveFs;
import io.activej.fs.FileMetadata;
import io.activej.fs.exception.*;
import io.activej.fs.exception.scalar.*;

import java.util.Map;
import java.util.Set;

import static io.activej.codec.StructuredCodecs.*;

public final class Codecs {
	public static final StructuredCodec<FileMetadata> FILE_META_CODEC = object(FileMetadata::parse,
			"size", FileMetadata::getSize, LONG_CODEC,
			"timestamp", FileMetadata::getTimestamp, LONG_CODEC);
	public static final StructuredCodec<FileMetadata> FILE_META_CODEC_NULLABLE = FILE_META_CODEC.nullable();
	public static final StructuredCodec<Map<String, FileMetadata>> FILE_META_MAP_CODEC = ofMap(STRING_CODEC, FILE_META_CODEC);
	public static final StructuredCodec<Set<String>> STRINGS_SET_CODEC = ofSet(STRING_CODEC);
	public static final StructuredCodec<Map<String, String>> SOURCE_TO_TARGET_CODEC = ofMap(STRING_CODEC, STRING_CODEC);

	// Components are serialized as ActiveFs.class to hide implementation details
	private static final StructuredCodec<Class<?>> COMPONENT_CODEC = StructuredCodec.ofObject(() -> ActiveFs.class);
	private static final StructuredCodec<FsScalarException> SCALAR_EXCEPTIONS_CODEC = simpleFsExceptionCodec(FsScalarException::new);
	public static final CodecSubtype<FsException> FS_EXCEPTION_CODEC = CodecSubtype.<FsException>create()
			.with(FsException.class, simpleFsExceptionCodec(FsException::new))
			.with(FsIOException.class, simpleFsExceptionCodec(FsIOException::new))
			.with(FsStateException.class, simpleFsExceptionCodec(FsStateException::new))
			.with(FsScalarException.class, SCALAR_EXCEPTIONS_CODEC)
			.with(MalformedGlobException.class, simpleFsExceptionCodec(MalformedGlobException::new))
			.with(ForbiddenPathException.class, simpleFsExceptionCodec(ForbiddenPathException::new))
			.with(IllegalOffsetException.class, simpleFsExceptionCodec(IllegalOffsetException::new))
			.with(IsADirectoryException.class, simpleFsExceptionCodec(IsADirectoryException::new))
			.with(PathContainsFileException.class, simpleFsExceptionCodec(PathContainsFileException::new))
			.with(FileNotFoundException.class, simpleFsExceptionCodec(FileNotFoundException::new))
			.with(FsBatchException.class, object(FsBatchException::new,
					"component", FsBatchException::getComponent, COMPONENT_CODEC,
					"exceptions", FsBatchException::getExceptions, StructuredCodecs.ofMap(STRING_CODEC, SCALAR_EXCEPTIONS_CODEC)));

	private static <T extends FsException> StructuredCodec<T> simpleFsExceptionCodec(TupleParser2<Class<?>, String, T> constructor) {
		return object(constructor,
				"component", FsException::getComponent, COMPONENT_CODEC,
				"message", FsException::getMessage, STRING_CODEC);
	}

}
