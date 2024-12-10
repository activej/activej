package io.activej.state.file;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.regex.Pattern;

public interface FileNamingScheme<R extends Comparable<R>> {
	boolean hasDiffsSupport();

	R nextRevision(@Nullable R previousRevision);

	default R firstRevision() {
		return nextRevision(null);
	}

	Pattern snapshotPattern();

	String snapshotPrefix();

	String encodeSnapshot(R revision);

	@Nullable R decodeSnapshot(String filename) throws IOException;

	Pattern diffPattern();

	String diffPrefix();

	String encodeDiff(R from, R to);

	@Nullable Diff<R> decodeDiff(String filename) throws IOException;

	record Diff<R extends Comparable<R>>(R from, R to) {}
}
