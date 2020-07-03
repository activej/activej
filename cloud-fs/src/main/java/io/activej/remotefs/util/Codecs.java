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

package io.activej.remotefs.util;

import io.activej.codec.StructuredCodec;
import io.activej.codec.StructuredCodecs;
import io.activej.common.tuple.Tuple1;
import io.activej.remotefs.FileMetadata;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.activej.codec.StructuredCodecs.*;

public final class Codecs {
	public static final StructuredCodec<FileMetadata> FILE_META_CODEC = StructuredCodecs.tuple(FileMetadata::parse,
			FileMetadata::getName, STRING_CODEC,
			FileMetadata::getSize, LONG_CODEC,
			FileMetadata::getTimestamp, LONG_CODEC);
	public static final StructuredCodec<FileMetadata> FILE_META_CODEC_NULLABLE = FILE_META_CODEC.nullable();
	public static final StructuredCodec<Tuple1<Integer>> ERROR_CODE_CODEC = object(Tuple1::new, "errorCode", Tuple1::getValue1, INT_CODEC);
	public static final StructuredCodec<List<FileMetadata>> FILE_META_LIST_CODEC = ofList(FILE_META_CODEC);
	public static final StructuredCodec<Map<String, @Nullable FileMetadata>> FILE_META_MAP_CODEC = ofMap(STRING_CODEC, FILE_META_CODEC_NULLABLE);
	public static final StructuredCodec<Set<String>> TO_DELETE_CODEC = ofSet(STRING_CODEC);
	public static final StructuredCodec<List<String>> NAMES_CODEC = ofList(STRING_CODEC);
	public static final StructuredCodec<Map<String, String>> SOURCE_TO_TARGET_CODEC = ofMap(STRING_CODEC, STRING_CODEC);

}
