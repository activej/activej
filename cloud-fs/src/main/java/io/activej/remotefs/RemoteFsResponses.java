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
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;

import static io.activej.codec.StructuredCodecs.*;
import static io.activej.common.collection.CollectionUtils.toLimitedString;
import static io.activej.remotefs.util.Codecs.FILE_META_CODEC;

public final class RemoteFsResponses {
	static final StructuredCodec<FsResponse> CODEC = CodecSubtype.<FsResponse>create()
			.with(UploadAck.class, object(UploadAck::new))
			.with(UploadFinished.class, object(UploadFinished::new))
			.with(DownloadSize.class, object(DownloadSize::new, "size", DownloadSize::getSize, LONG_CODEC))
			.with(MoveFinished.class, object(MoveFinished::new))
			.with(MoveAllFinished.class, object(MoveAllFinished::new))
			.with(CopyFinished.class, object(CopyFinished::new))
			.with(CopyAllFinished.class, object(CopyAllFinished::new))
			.with(DeleteFinished.class, object(DeleteFinished::new))
			.with(DeleteAllFinished.class, object(DeleteAllFinished::new))
			.with(ListFinished.class, object(ListFinished::new, "files", ListFinished::getFiles, ofList(FILE_META_CODEC)))
			.with(InspectFinished.class, object(InspectFinished::new, "metadata", InspectFinished::getMetadata, FILE_META_CODEC.nullable()))
			.with(InspectAllFinished.class, object(InspectAllFinished::new, "metadataList", InspectAllFinished::getMetadataList, ofList(FILE_META_CODEC)))
			.with(PingFinished.class, object(PingFinished::new))
			.with(ServerError.class, object(ServerError::new, "code", ServerError::getCode, INT_CODEC));

	public abstract static class FsResponse {
	}

	public static final class UploadAck extends FsResponse {
		@Override
		public String toString() {
			return "UploadAck{}";
		}
	}

	public static final class UploadFinished extends FsResponse {
		@Override
		public String toString() {
			return "UploadFinished{}";
		}
	}

	public static final class DownloadSize extends FsResponse {
		private final long size;

		public DownloadSize(long size) {
			this.size = size;
		}

		public long getSize() {
			return size;
		}

		@Override
		public String toString() {
			return "DownloadSize{size=" + size + '}';
		}
	}

	public static final class CopyFinished extends FsResponse {
		@Override
		public String toString() {
			return "CopyFinished{}";
		}
	}

	public static final class CopyAllFinished extends FsResponse {
		@Override
		public String toString() {
			return "CopyAllFinished{}";
		}
	}

	public static final class MoveFinished extends FsResponse {
		@Override
		public String toString() {
			return "MoveFinished{}";
		}
	}

	public static final class MoveAllFinished extends FsResponse {
		@Override
		public String toString() {
			return "MoveAllFinished{}";
		}
	}

	public static final class ListFinished extends FsResponse {
		private final List<FileMetadata> files;

		public ListFinished(List<FileMetadata> files) {
			this.files = Collections.unmodifiableList(files);
		}

		public List<FileMetadata> getFiles() {
			return files;
		}

		@Override
		public String toString() {
			return "ListFinished{files=" + files.size() + '}';
		}
	}

	public static final class DeleteFinished extends FsResponse {
		@Override
		public String toString() {
			return "DeleteFinished{}";
		}
	}

	public static final class DeleteAllFinished extends FsResponse {
		@Override
		public String toString() {
			return "DeleteAllFinished{}";
		}
	}

	public static final class ServerError extends FsResponse {
		private final int code;

		public ServerError(int code) {
			this.code = code;
		}

		public int getCode() {
			return code;
		}

		@Override
		public String toString() {
			return "ServerError{code=" + code + '}';
		}
	}

	public static final class InspectFinished extends FsResponse {
		@Nullable
		private final FileMetadata metadata;

		public InspectFinished(@Nullable FileMetadata metadata) {
			this.metadata = metadata;
		}

		@Nullable
		public FileMetadata getMetadata() {
			return metadata;
		}

		@Override
		public String toString() {
			return "InspectFinished{metadata=" + metadata + '}';
		}
	}

	public static final class InspectAllFinished extends FsResponse {
		private final List<FileMetadata> metadataList;

		public InspectAllFinished(List<FileMetadata> metadataList) {
			this.metadataList = metadataList;
		}

		@Nullable
		public List<FileMetadata> getMetadataList() {
			return metadataList;
		}

		@Override
		public String toString() {
			return "InspectAllFinished{metadataList=" + toLimitedString(metadataList, 100) + '}';
		}
	}

	public static final class PingFinished extends FsResponse {
		@Override
		public String toString() {
			return "PingFinished{}";
		}
	}
}
