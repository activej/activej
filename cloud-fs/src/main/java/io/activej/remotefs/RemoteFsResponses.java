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
import io.activej.codec.StructuredCodecs;

import java.util.Collections;
import java.util.List;

import static io.activej.codec.StructuredCodecs.*;

public final class RemoteFsResponses {
	public static final StructuredCodec<FileMetadata> FILE_META_CODEC = StructuredCodecs.tuple(FileMetadata::parse,
			FileMetadata::getName, STRING_CODEC,
			FileMetadata::getSize, LONG_CODEC,
			FileMetadata::getTimestamp, LONG_CODEC);

	static final StructuredCodec<FsResponse> CODEC = CodecSubtype.<FsResponse>create()
			.with(UploadAck.class, object(UploadAck::new, "ok", UploadAck::isOk, BOOLEAN_CODEC))
			.with(UploadFinished.class, object(UploadFinished::new))
			.with(DownloadSize.class, object(DownloadSize::new, "size", DownloadSize::getSize, LONG_CODEC))
			.with(MoveFinished.class, object(MoveFinished::new))
			.with(CopyFinished.class, object(CopyFinished::new))
			.with(DeleteFinished.class, object(DeleteFinished::new))
			.with(ListFinished.class, object(ListFinished::new, "files", ListFinished::getFiles, ofList(FILE_META_CODEC)))
			.with(ServerError.class, object(ServerError::new, "code", ServerError::getCode, INT_CODEC));

	public abstract static class FsResponse {
	}

	public static class UploadAck extends FsResponse {
		private final boolean ok;

		public UploadAck(boolean ok) {
			this.ok = ok;
		}

		public boolean isOk() {
			return ok;
		}

		@Override
		public String toString() {
			return "UploadAck{od=" + ok + '}';
		}
	}

	public static class UploadFinished extends FsResponse {
		@Override
		public String toString() {
			return "UploadFinished{}";
		}
	}

	public static class DownloadSize extends FsResponse {
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

	public static class MoveFinished extends FsResponse {
		public MoveFinished() {
		}

		@Override
		public String toString() {
			return "MoveFinished{}";
		}
	}

	public static class CopyFinished extends FsResponse {
		public CopyFinished() {
		}

		@Override
		public String toString() {
			return "CopyFinished{}";
		}
	}

	public static class ListFinished extends FsResponse {
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

	public static class DeleteFinished extends FsResponse {
		public DeleteFinished() {
		}

		@Override
		public String toString() {
			return "DeleteFinished{}";
		}
	}

	public static class ServerError extends FsResponse {
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
}
