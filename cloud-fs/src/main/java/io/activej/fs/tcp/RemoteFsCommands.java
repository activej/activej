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

package io.activej.fs.tcp;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;

import static io.activej.common.collection.CollectionUtils.toLimitedString;

@SuppressWarnings("WeakerAccess")
public final class RemoteFsCommands {

	@CompiledJson(discriminator = "Type")
	public abstract static class FsCommand {
	}

	@CompiledJson(name = "Upload")
	public static final class Upload extends FsCommand {
		private final String name;
		@Nullable
		private final Long size;

		public Upload(String name, @Nullable Long size) {
			this.name = name;
			this.size = size;
		}

		public String getName() {
			return name;
		}

		public @Nullable Long getSize() {
			return size;
		}

		@Override
		public String toString() {
			return "Upload{name='" + name + "', size=" + size + '}';
		}
	}

	@CompiledJson(name = "Append")
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

	@CompiledJson(name = "Download")
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

	@CompiledJson(name = "Copy")
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

	@CompiledJson(name = "CopyAll")
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

	@CompiledJson(name = "Move")
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

	@CompiledJson(name = "MoveAll")
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

	@CompiledJson(name = "Delete")
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

	@CompiledJson(name = "DeleteAll")
	public static final class DeleteAll extends FsCommand {
		private final Set<String> toDelete;

		public DeleteAll(Set<String> filesToDelete) {
			this.toDelete = filesToDelete;
		}

		@JsonAttribute(name = "toDelete")
		public Set<String> getFilesToDelete() {
			return toDelete;
		}

		@Override
		public String toString() {
			return "DeleteAll{toDelete=" + toLimitedString(toDelete, 100) + '}';
		}
	}

	@CompiledJson(name = "List")
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

	@CompiledJson(name = "Info")
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

	@CompiledJson(name = "InfoAll")
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

	@CompiledJson(name = "Ping")
	public static final class Ping extends FsCommand {
		@Override
		public String toString() {
			return "Ping{}";
		}
	}
}
