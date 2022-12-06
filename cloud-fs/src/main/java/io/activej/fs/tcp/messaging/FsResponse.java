package io.activej.fs.tcp.messaging;

import io.activej.fs.FileMetadata;
import io.activej.fs.exception.FsException;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public sealed interface FsResponse permits
		FsResponse.AppendAck,
		FsResponse.AppendFinished,
		FsResponse.CopyAllFinished,
		FsResponse.CopyFinished,
		FsResponse.DeleteAllFinished,
		FsResponse.DeleteFinished,
		FsResponse.DownloadSize,
		FsResponse.Handshake,
		FsResponse.InfoAllFinished,
		FsResponse.InfoFinished,
		FsResponse.ListFinished,
		FsResponse.MoveAllFinished,
		FsResponse.MoveFinished,
		FsResponse.Pong,
		FsResponse.ServerError,
		FsResponse.UploadAck,
		FsResponse.UploadFinished {

	record UploadAck() implements FsResponse {
	}

	record UploadFinished() implements FsResponse {
	}

	record AppendAck() implements FsResponse {
	}

	record AppendFinished() implements FsResponse {
	}

	record DownloadSize(long size) implements FsResponse {
	}

	record CopyFinished() implements FsResponse {
	}

	record CopyAllFinished() implements FsResponse {
	}

	record MoveFinished() implements FsResponse {
	}

	record MoveAllFinished() implements FsResponse {
	}

	record ListFinished(Map<String, FileMetadata> files) implements FsResponse {
	}

	record InfoFinished(@Nullable FileMetadata fileMetadata) implements FsResponse {
	}

	record InfoAllFinished(Map<String, FileMetadata> files) implements FsResponse {
	}

	record DeleteFinished() implements FsResponse {
	}

	record DeleteAllFinished() implements FsResponse {
	}

	record Pong() implements FsResponse {
	}

	record ServerError(FsException exception) implements FsResponse {
	}

	record HandshakeFailure(Version minimalVersion, String message) {
	}

	record Handshake(@Nullable HandshakeFailure handshakeFailure) implements FsResponse {
	}
}
