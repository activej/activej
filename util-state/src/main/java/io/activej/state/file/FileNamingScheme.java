package io.activej.state.file;

import org.jetbrains.annotations.Nullable;

public interface FileNamingScheme {
	String snapshotGlob();

	String encodeSnapshot(long revision);

	@Nullable Long decodeSnapshot(String filename);
}
