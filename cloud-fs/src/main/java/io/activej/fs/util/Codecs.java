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

import io.activej.codec.StructuredCodec;
import io.activej.fs.FileMetadata;

import java.util.Map;
import java.util.Set;

import static io.activej.codec.StructuredCodecs.*;

public final class Codecs {
	public static final StructuredCodec<FileMetadata> FILE_META_CODEC = object(FileMetadata::decode,
			"size", FileMetadata::getSize, LONG_CODEC,
			"timestamp", FileMetadata::getTimestamp, LONG_CODEC);
	public static final StructuredCodec<FileMetadata> FILE_META_CODEC_NULLABLE = FILE_META_CODEC.nullable();
	public static final StructuredCodec<Map<String, FileMetadata>> FILE_META_MAP_CODEC = ofMap(STRING_CODEC, FILE_META_CODEC);
	public static final StructuredCodec<Set<String>> STRINGS_SET_CODEC = ofSet(STRING_CODEC);
	public static final StructuredCodec<Map<String, String>> SOURCE_TO_TARGET_CODEC = ofMap(STRING_CODEC, STRING_CODEC);
}
