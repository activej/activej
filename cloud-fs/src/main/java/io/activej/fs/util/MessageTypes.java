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

import io.activej.fs.FileMetadata;
import io.activej.types.TypeT;

import java.util.Map;
import java.util.Set;

public final class MessageTypes {
	public static final TypeT<Set<String>> STRING_SET_TYPE = new TypeT<>() {};
	public static final TypeT<Map<String, String>> STRING_STRING_MAP_TYPE = new TypeT<>() {};
	public static final TypeT<Map<String, FileMetadata>> STRING_META_MAP_TYPE = new TypeT<>() {};
}
