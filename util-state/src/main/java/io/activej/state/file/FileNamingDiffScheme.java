package io.activej.state.file;

import org.jetbrains.annotations.Nullable;

public interface FileNamingDiffScheme extends FileNamingScheme {
	String diffGlob();

	String diffGlob(long from);

	String encodeDiff(long from, long to);

	@Nullable Diff decodeDiff(String filename);

	record Diff(long from, long to) {
	}
}
