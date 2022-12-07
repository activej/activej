package io.activej.crdt.messaging;

import org.jetbrains.annotations.Nullable;

public sealed interface CrdtResponse permits
		CrdtResponse.DownloadStarted,
		CrdtResponse.Handshake,
		CrdtResponse.Pong,
		CrdtResponse.RemoveAck,
		CrdtResponse.ServerError,
		CrdtResponse.TakeStarted,
		CrdtResponse.UploadAck {

	record UploadAck() implements CrdtResponse {
	}

	record RemoveAck() implements CrdtResponse {
	}

	record Pong() implements CrdtResponse {
	}

	record DownloadStarted() implements CrdtResponse {
	}

	record TakeStarted() implements CrdtResponse {
	}

	record ServerError(String message) implements CrdtResponse {
	}

	record HandshakeFailure(Version minimalVersion, String message) {
	}

	record Handshake(@Nullable HandshakeFailure handshakeFailure) implements CrdtResponse {
	}
}
