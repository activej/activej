package io.activej.state.file;

import org.jetbrains.annotations.Nullable;

public interface FileNamingScheme {
	String snapshotGlob();

	String encodeSnapshot(long revision);

	@Nullable Long decodeSnapshot(String filename);

	String diffGlob();

	String diffGlob(long from);

	String encodeDiff(long from, long to);

	@Nullable Diff decodeDiff(String filename);

	record Diff(long from, long to) {
	}
}
