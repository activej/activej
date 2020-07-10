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

import java.util.Map;
import java.util.Set;

import static io.activej.codec.StructuredCodecs.*;
import static io.activej.common.collection.CollectionUtils.toLimitedString;
import static io.activej.remotefs.util.Codecs.SOURCE_TO_TARGET_CODEC;
import static io.activej.remotefs.util.Codecs.STRINGS_SET_CODEC;

@SuppressWarnings("WeakerAccess")
public final class RemoteFsCommands {

	static final StructuredCodec<FsCommand> CODEC = CodecSubtype.<FsCommand>create()
			.with(Upload.class, object(Upload::new,
					"name", Upload::getName, STRING_CODEC,
					"size", Upload::getSize, LONG_CODEC))
			.with(Append.class, object(Append::new,
					"name", Append::getName, STRING_CODEC,
					"offset", Append::getOffset, LONG_CODEC))
			.with(Download.class, object(Download::new,
					"name", Download::getName, STRING_CODEC,
					"offset", Download::getOffset, LONG_CODEC,
					"limit", Download::getLimit, LONG_CODEC))
			.with(Copy.class, object(Copy::new,
					"name", Copy::getName, STRING_CODEC,
					"target", Copy::getTarget, STRING_CODEC))
			.with(CopyAll.class, object(CopyAll::new,
					"sourceToTarget", CopyAll::getSourceToTarget, SOURCE_TO_TARGET_CODEC))
			.with(Move.class, object(Move::new,
					"name", Move::getName, STRING_CODEC,
					"target", Move::getTarget, STRING_CODEC))
			.with(MoveAll.class, object(MoveAll::new,
					"sourceToTarget", MoveAll::getSourceToTarget, SOURCE_TO_TARGET_CODEC))
			.with(Delete.class, object(Delete::new,
					"name", Delete::getName, STRING_CODEC))
			.with(DeleteAll.class, object(DeleteAll::new,
					"toDelete", DeleteAll::getFilesToDelete, STRINGS_SET_CODEC))
			.with(List.class, object(List::new,
					"glob", List::getGlob, STRING_CODEC))
			.with(Info.class, object(Info::new,
					"name", Info::getName, STRING_CODEC))
			.with(InfoAll.class, object(InfoAll::new,
					"names", InfoAll::getNames, STRINGS_SET_CODEC))
			.with(Ping.class, object(Ping::new));

	public abstract static class FsCommand {
	}

	public static final class Upload extends FsCommand {
		private final String name;
		private final long size;

		public Upload(String name, long size) {
			this.name = name;
			this.size = size;
		}

		public String getName() {
			return name;
		}

		public Long getSize() {
			return size;
		}

		@Override
		public String toString() {
			return "Upload{name='" + name + "', size=" + size + '}';
		}
	}

	public static final class Append extends FsCommand {
		private final String name;
		private final long offset;

		public Append(String name, long offset) {
			this.name = name;
			this.offset = offset;
		}

		public String getName() {
			return name;
		}

		public long getOffset() {
			return offset;
		}

		@Override
		public String toString() {
			return "Append{name='" + name + "', offset=" + offset + '}';
		}
	}

	public static final class Download extends FsCommand {
		private final String name;
		private final long offset;
		private final long limit;

		public Download(String name, long offset, long limit) {
			this.name = name;
			this.offset = offset;
			this.limit = limit;
		}

		public String getName() {
			return name;
		}

		public long getOffset() {
			return offset;
		}

		public long getLimit() {
			return limit;
		}

		@Override
		public String toString() {
			return "Download{name='" + name + "', offset=" + offset + ", limit=" + limit + '}';
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

	public static final class CopyAll extends FsCommand {
		private final Map<String, String> sourceToTarget;

		public CopyAll(Map<String, String> sourceToTarget) {
			this.sourceToTarget = sourceToTarget;
		}

		public Map<String, String> getSourceToTarget() {
			return sourceToTarget;
		}

		@Override
		public String toString() {
			return "CopyAll{sourceToTarget=" + toLimitedString(sourceToTarget, 50) + '}';
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

	public static final class MoveAll extends FsCommand {
		private final Map<String, String> sourceToTarget;

		public MoveAll(Map<String, String> sourceToTarget) {
			this.sourceToTarget = sourceToTarget;
		}

		public Map<String, String> getSourceToTarget() {
			return sourceToTarget;
		}

		@Override
		public String toString() {
			return "MoveAll{sourceToTarget=" + toLimitedString(sourceToTarget, 50) + '}';
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

	public static final class DeleteAll extends FsCommand {
		private final Set<String> toDelete;

		public DeleteAll(Set<String> toDelete) {
			this.toDelete = toDelete;
		}

		public Set<String> getFilesToDelete() {
			return toDelete;
		}

		@Override
		public String toString() {
			return "DeleteAll{toDelete=" + toLimitedString(toDelete, 100) + '}';
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

	public static final class Info extends FsCommand {
		private final String name;

		public Info(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		@Override
		public String toString() {
			return "Info{name='" + name + "'}";
		}
	}

	public static final class InfoAll extends FsCommand {
		private final Set<String> names;

		public InfoAll(Set<String> names) {
			this.names = names;
		}

		public Set<String> getNames() {
			return names;
		}

		@Override
		public String toString() {
			return "InfoAll{names='" + toLimitedString(names, 100) + "'}";
		}
	}

	public static final class Ping extends FsCommand {
		@Override
		public String toString() {
			return "Ping{}";
		}
	}
}
