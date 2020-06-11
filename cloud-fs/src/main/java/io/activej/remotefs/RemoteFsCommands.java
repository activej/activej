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
					"name", Upload::getName, STRING_CODEC))
			.with(Download.class, object(Download::new,
					"name", Download::getName, STRING_CODEC,
					"offset", Download::getOffset, LONG_CODEC,
					"length", Download::getLength, LONG_CODEC))
			.with(Move.class, object(Move::new,
					"name", Move::getName, STRING_CODEC,
					"target", Move::getTarget, STRING_CODEC))
			.with(Copy.class, object(Copy::new,
					"name", Copy::getName, STRING_CODEC,
					"target", Copy::getTarget, STRING_CODEC))
			.with(Delete.class, object(Delete::new,
					"name", Delete::getName, STRING_CODEC))
			.with(List.class, object(List::new,
					"glob", List::getGlob, STRING_CODEC));

	public abstract static class FsCommand {
	}

	public static final class Upload extends FsCommand {
		private final String name;

		public Upload(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		@Override
		public String toString() {
			return "Upload{name='" + name + '}';
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

		public Move(String name, String target) {
			this.name = name;
			this.target = target;
		}

		public String getName() {
			return name;
		}

		public String getTarget() {
			return target;
		}

		@Override
		public String toString() {
			return "Move{name=" + name + ", target=" + target + '}';
		}
	}

	public static final class Copy extends FsCommand {
		private final String name;
		private final String target;

		public Copy(String name, String target) {
			this.name = name;
			this.target = target;
		}

		public String getName() {
			return name;
		}

		public String getTarget() {
			return target;
		}

		@Override
		public String toString() {
			return "Copy{name=" + name + ", target=" + target + '}';
		}
	}

	public static final class Delete extends FsCommand {
		private final String name;

		public Delete(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		@Override
		public String toString() {
			return "Delete{name='" + name + '}';
		}
	}

	public static final class List extends FsCommand {
		private final String glob;

		public List(String glob) {
			this.glob = glob;
		}

		public String getGlob() {
			return glob;
		}

		@Override
		public String toString() {
			return "List{glob='" + glob + "'}";
		}
	}
}
