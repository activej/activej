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

import io.activej.codec.CodecSubtype;
import io.activej.codec.StructuredCodec;

import static io.activej.codec.StructuredCodecs.*;

@SuppressWarnings("WeakerAccess")
public final class RemoteFsCommands {

	static final StructuredCodec<FsCommand> CODEC = CodecSubtype.<FsCommand>create()
			.with(Upload.class, object(Upload::new,
					"name", Upload::getName, STRING_CODEC,
					"targetRevision", Upload::getRevision, LONG_CODEC))
			.with(Download.class, object(Download::new,
					"name", Download::getName, STRING_CODEC,
					"offset", Download::getOffset, LONG_CODEC,
					"length", Download::getLength, LONG_CODEC))
			.with(Move.class, object(Move::new,
					"name", Move::getName, STRING_CODEC,
					"target", Move::getTarget, STRING_CODEC,
					"targetRevision", Move::getTargetRevision, LONG_CODEC,
					"removeRevision", Move::getRemoveRevision, LONG_CODEC))
			.with(Copy.class, object(Copy::new,
					"name", Copy::getName, STRING_CODEC,
					"target", Copy::getTarget, STRING_CODEC,
					"targetRevision", Copy::getRevision, LONG_CODEC))
			.with(Delete.class, object(Delete::new,
					"name", Delete::getName, STRING_CODEC,
					"targetRevision", Delete::getRevision, LONG_CODEC))
			.with(List.class, object(List::new,
					"glob", List::getGlob, STRING_CODEC,
					"tombstones", List::needTombstones, BOOLEAN_CODEC));

	public abstract static class FsCommand {
	}

	public static final class Upload extends FsCommand {
		private final String name;
		private final long revision;

		public Upload(String name, long revision) {
			this.name = name;
			this.revision = revision;
		}

		public String getName() {
			return name;
		}

		public long getRevision() {
			return revision;
		}

		@Override
		public String toString() {
			return "Upload{name='" + name + "', revision=" + revision + '}';
		}
	}

	public static final class Download extends FsCommand {
		private final String name;
		private final long offset;
		private final long length;

		public Download(String name, long offset, long length) {
			this.name = name;
			this.offset = offset;
			this.length = length;
		}

		public String getName() {
			return name;
		}

		public long getOffset() {
			return offset;
		}

		public long getLength() {
			return length;
		}

		@Override
		public String toString() {
			return "Download{name='" + name + "', offset=" + offset + ", length=" + length + '}';
		}
	}

	public static final class Move extends FsCommand {
		private final String name;
		private final String target;
		private final long targetRevision;
		private final long removeRevision;

		public Move(String name, String target, long targetRevision, long removeRevision) {
			this.name = name;
			this.target = target;
			this.targetRevision = targetRevision;
			this.removeRevision = removeRevision;
		}

		public String getName() {
			return name;
		}

		public String getTarget() {
			return target;
		}

		public long getTargetRevision() {
			return targetRevision;
		}

		public long getRemoveRevision() {
			return removeRevision;
		}

		@Override
		public String toString() {
			return "Move{name=" + name + ", target=" + target + ", targetRevision=" + targetRevision + '}';
		}
	}

	public static final class Copy extends FsCommand {
		private final String name;
		private final String target;
		private final long revision;

		public Copy(String name, String target, long revision) {
			this.name = name;
			this.target = target;
			this.revision = revision;
		}

		public String getName() {
			return name;
		}

		public String getTarget() {
			return target;
		}

		public long getRevision() {
			return revision;
		}

		@Override
		public String toString() {
			return "Copy{name=" + name + ", target=" + target + ", revision=" + revision + '}';
		}
	}

	public static final class Delete extends FsCommand {
		private final String name;
		private final long revision;

		public Delete(String name, long revision) {
			this.name = name;
			this.revision = revision;
		}

		public String getName() {
			return name;
		}

		public long getRevision() {
			return revision;
		}

		@Override
		public String toString() {
			return "Delete{name='" + name + "', revision=" + revision + '}';
		}
	}

	public static final class List extends FsCommand {
		private final String glob;
		private final boolean tombstones;

		public List(String glob, boolean tombstones) {
			this.glob = glob;
			this.tombstones = tombstones;
		}

		public String getGlob() {
			return glob;
		}

		public boolean needTombstones() {
			return tombstones;
		}

		@Override
		public String toString() {
			return "List{glob='" + glob + "'}";
		}
	}
}
