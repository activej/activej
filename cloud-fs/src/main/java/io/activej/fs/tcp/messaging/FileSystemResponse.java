package io.activej.fs.tcp.messaging;

import io.activej.fs.FileMetadata;
import io.activej.fs.exception.FileSystemException;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public sealed interface FileSystemResponse permits
	FileSystemResponse.AppendAck,
	FileSystemResponse.AppendFinished,
	FileSystemResponse.CopyAllFinished,
	FileSystemResponse.CopyFinished,
	FileSystemResponse.DeleteAllFinished,
	FileSystemResponse.DeleteFinished,
	FileSystemResponse.DownloadSize,
	FileSystemResponse.Handshake,
	FileSystemResponse.InfoAllFinished,
	FileSystemResponse.InfoFinished,
	FileSystemResponse.ListFinished,
	FileSystemResponse.MoveAllFinished,
	FileSystemResponse.MoveFinished,
	FileSystemResponse.Pong,
	FileSystemResponse.ServerError,
	FileSystemResponse.UploadAck,
	FileSystemResponse.UploadFinished {

	record UploadAck() implements FileSystemResponse {
	}

	record UploadFinished() implements FileSystemResponse {
	}

	record AppendAck() implements FileSystemResponse {
	}

	record AppendFinished() implements FileSystemResponse {
	}

	record DownloadSize(long size) implements FileSystemResponse {
	}

	record CopyFinished() implements FileSystemResponse {
	}

	record CopyAllFinished() implements FileSystemResponse {
	}

	record MoveFinished() implements FileSystemResponse {
	}

	record MoveAllFinished() implements FileSystemResponse {
	}

	record ListFinished(Map<String, FileMetadata> files) implements FileSystemResponse {
	}

	record InfoFinished(@Nullable FileMetadata fileMetadata) implements FileSystemResponse {
	}

	record InfoAllFinished(Map<String, FileMetadata> files) implements FileSystemResponse {
	}

	record DeleteFinished() implements FileSystemResponse {
	}

	record DeleteAllFinished() implements FileSystemResponse {
	}

	record Pong() implements FileSystemResponse {
	}

	record ServerError(FileSystemException exception) implements FileSystemResponse {
	}

	record HandshakeFailure(Version minimalVersion, String message) {
	}

	record Handshake(@Nullable HandshakeFailure handshakeFailure) implements FileSystemResponse {
	}
}
