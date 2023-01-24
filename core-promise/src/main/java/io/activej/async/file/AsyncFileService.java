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

package io.activej.async.file;

import io.activej.common.annotation.ComponentInterface;
import io.activej.promise.Promise;

import java.nio.channels.FileChannel;

@ComponentInterface
public interface AsyncFileService {
	Promise<Integer> read(FileChannel channel, long position, byte[] array, int offset, int size);

	Promise<Integer> write(FileChannel channel, long position, byte[] array, int offset, int size);
}
