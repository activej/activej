package io.activej.crdt.messaging;

public sealed interface CrdtRequest permits
		CrdtRequest.Download,
		CrdtRequest.Handshake,
		CrdtRequest.Ping,
		CrdtRequest.Remove,
		CrdtRequest.Take,
		CrdtRequest.TakeAck,
		CrdtRequest.Upload {

	record Upload() implements CrdtRequest {
	}

	record Remove() implements CrdtRequest {
	}

	record Ping() implements CrdtRequest {
	}

	record Take() implements CrdtRequest {
	}

	record TakeAck() implements CrdtRequest {
	}

	record Download(long token) implements CrdtRequest {
	}

	record Handshake(Version version) implements CrdtRequest {
	}
}
