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

package io.activej.fs.exception;

import io.activej.codec.CodecSubtype;
import io.activej.codec.StructuredCodec;
import io.activej.codec.StructuredCodecs;
import io.activej.common.tuple.TupleDecoder2;

import static io.activej.codec.StructuredCodecs.STRING_CODEC;
import static io.activej.codec.StructuredCodecs.object;

public final class FsExceptionCodec {
	private static final CodecSubtype<FsScalarException> SCALAR_EXCEPTION_CODEC_SUBTYPE = CodecSubtype.<FsScalarException>create()
			.with(FsScalarException.class, simpleFsExceptionCodec(FsScalarException::new))
			.with(MalformedGlobException.class, simpleFsExceptionCodec(MalformedGlobException::new))
			.with(ForbiddenPathException.class, simpleFsExceptionCodec(ForbiddenPathException::new))
			.with(IllegalOffsetException.class, simpleFsExceptionCodec(IllegalOffsetException::new))
			.with(IsADirectoryException.class, simpleFsExceptionCodec(IsADirectoryException::new))
			.with(PathContainsFileException.class, simpleFsExceptionCodec(PathContainsFileException::new))
			.with(FileNotFoundException.class, simpleFsExceptionCodec(FileNotFoundException::new));

	public static final CodecSubtype<FsException> CODEC = CodecSubtype.<FsException>create()
			.with(FsException.class, simpleFsExceptionCodec(FsException::new))
			.with(FsIOException.class, simpleFsExceptionCodec(FsIOException::new))
			.with(FsStateException.class, simpleFsExceptionCodec(FsStateException::new))
			.with(FsScalarException.class, SCALAR_EXCEPTION_CODEC_SUBTYPE)
			.with(FsBatchException.class, object(exceptions -> new FsBatchException(exceptions, false),
					"exceptions", FsBatchException::getExceptions, StructuredCodecs.ofMap(STRING_CODEC, SCALAR_EXCEPTION_CODEC_SUBTYPE)));

	private static <T extends FsException> StructuredCodec<T> simpleFsExceptionCodec(TupleDecoder2<String, Boolean, T> constructor) {
		return object(message -> constructor.create(message, false),
				"message", FsException::getMessage, STRING_CODEC);
	}
}
