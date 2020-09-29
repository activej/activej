package io.activej.state.file;

import org.jetbrains.annotations.Nullable;

public interface FileNamingScheme {
	String snapshotGlob();

	String encodeSnapshot(long revision);

	@Nullable
	Long decodeSnapshot(String filename);

	String diffGlob();

	String diffGlob(long from);

	String encodeDiff(long from, long to);

	@Nullable
	Diff decodeDiff(String filename);

	final class Diff {
		private final long from;
		private final long to;

		public Diff(long from, long to) {
			this.from = from;
			this.to = to;
		}

		public long getFrom() {
			return from;
		}

		public long getTo() {
			return to;
		}
	}

}
