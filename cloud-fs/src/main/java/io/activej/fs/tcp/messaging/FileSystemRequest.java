package io.activej.fs.tcp.messaging;

import java.util.Map;
import java.util.Set;

public sealed interface FileSystemRequest permits
		FileSystemRequest.Append,
		FileSystemRequest.Copy,
		FileSystemRequest.CopyAll,
		FileSystemRequest.Delete,
		FileSystemRequest.DeleteAll,
		FileSystemRequest.Download,
		FileSystemRequest.Handshake,
		FileSystemRequest.Info,
		FileSystemRequest.InfoAll,
		FileSystemRequest.List,
		FileSystemRequest.Move,
		FileSystemRequest.MoveAll,
		FileSystemRequest.Ping,
		FileSystemRequest.Upload {

	record Handshake(Version version) implements FileSystemRequest {
	}

	record Upload(String name, long size) implements FileSystemRequest {
	}

	record Append(String name, long offset) implements FileSystemRequest {
	}

	record Download(String name, long offset, long limit) implements FileSystemRequest {
	}

	record Copy(String name, String target) implements FileSystemRequest {
	}

	record CopyAll(Map<String, String> sourceToTarget) implements FileSystemRequest {
	}

	record Move(String name, String target) implements FileSystemRequest {
	}

	record MoveAll(Map<String, String> sourceToTarget) implements FileSystemRequest {
	}

	record Delete(String name) implements FileSystemRequest {
	}

	record DeleteAll(Set<String> toDelete) implements FileSystemRequest {
	}

	record List(String glob) implements FileSystemRequest {
	}

	record Info(String name) implements FileSystemRequest {
	}

	record InfoAll(Set<String> names) implements FileSystemRequest {
	}

	record Ping() implements FileSystemRequest {
	}
}
