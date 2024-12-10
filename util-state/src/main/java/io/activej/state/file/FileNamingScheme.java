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

	Pattern snapshotGlob();

	String encodeSnapshot(R revision);

	R decodeSnapshot(String filename) throws IOException;

	Pattern diffGlob();

	String encodeDiff(R from, R to);

	record Diff<R extends Comparable<R>>(R from, R to) {}

	Diff<R> decodeDiff(String filename) throws IOException;
}
