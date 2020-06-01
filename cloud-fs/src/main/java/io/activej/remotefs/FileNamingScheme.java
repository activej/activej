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

package io.activej.remotefs;

import java.nio.file.Path;

public interface FileNamingScheme {

	String encode(String name, long revision, boolean tombstone);

	FilenameInfo decode(Path path, String name);

	class FilenameInfo {
		private final Path filePath;
		private final String name;
		private final long revision;
		private final boolean tombstone;

		public FilenameInfo(Path filePath, String name, long revision, boolean tombstone) {
			this.filePath = filePath;
			this.name = name;
			this.revision = revision;
			this.tombstone = tombstone;
		}

		public Path getFilePath() {
			return filePath;
		}

		public String getName() {
			return name;
		}

		public long getRevision() {
			return revision;
		}

		public boolean isTombstone() {
			return tombstone;
		}
	}
}
