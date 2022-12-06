package io.activej.fs.tcp.messaging;

import java.util.Map;
import java.util.Set;

public sealed interface FsRequest permits
		FsRequest.Append,
		FsRequest.Copy,
		FsRequest.CopyAll,
		FsRequest.Delete,
		FsRequest.DeleteAll,
		FsRequest.Download,
		FsRequest.Handshake,
		FsRequest.Info,
		FsRequest.InfoAll,
		FsRequest.List,
		FsRequest.Move,
		FsRequest.MoveAll,
		FsRequest.Ping,
		FsRequest.Upload {

	record Handshake(Version version) implements FsRequest {
	}

	record Upload(String name, long size) implements FsRequest {
	}

	record Append(String name, long offset) implements FsRequest {
	}

	record Download(String name, long offset, long limit) implements FsRequest {
	}

	record Copy(String name, String target) implements FsRequest {
	}

	record CopyAll(Map<String, String> sourceToTarget) implements FsRequest {
	}

	record Move(String name, String target) implements FsRequest {
	}

	record MoveAll(Map<String, String> sourceToTarget) implements FsRequest {
	}

	record Delete(String name) implements FsRequest {
	}

	record DeleteAll(Set<String> toDelete) implements FsRequest {
	}

	record List(String glob) implements FsRequest {
	}

	record Info(String name) implements FsRequest {
	}

	record InfoAll(Set<String> names) implements FsRequest {
	}

	record Ping() implements FsRequest {
	}
}
