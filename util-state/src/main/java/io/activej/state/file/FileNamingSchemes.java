package io.activej.state.file;

import org.intellij.lang.annotations.RegExp;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

public class FileNamingSchemes {

	private static final @RegExp String INTEGER_REVISION_PATTERN = "[0-9]+";

	public static FileNamingScheme<Long> ofLong(String prefix, String suffix) {
		return new FileNamingSchemeImpl<>(RevisionParser.ofLong(), r -> r == null ? 1 : r + 1, prefix, suffix, INTEGER_REVISION_PATTERN);
	}

	public static FileNamingScheme<Long> ofLong(String prefix, String suffix, String diffPrefix, String diffSuffix, String diffSeparator) {
		return new FileNamingSchemeImpl<>(RevisionParser.ofLong(), r -> r == null ? 1 : r + 1, prefix, suffix, INTEGER_REVISION_PATTERN, diffPrefix, diffSuffix, diffSeparator);
	}

	public static FileNamingScheme<Integer> ofInteger(String prefix, String suffix) {
		return new FileNamingSchemeImpl<>(RevisionParser.ofInteger(), r -> r == null ? 1 : r + 1, prefix, suffix, INTEGER_REVISION_PATTERN);
	}

	public static FileNamingScheme<Integer> ofInteger(String prefix, String suffix, String diffPrefix, String diffSuffix, String diffSeparator) {
		return new FileNamingSchemeImpl<>(RevisionParser.ofInteger(), r -> r == null ? 1 : r + 1, prefix, suffix, INTEGER_REVISION_PATTERN, diffPrefix, diffSuffix, diffSeparator);
	}

	public static <R extends Comparable<R>> FileNamingScheme<R> of(
		RevisionParser<R> revisionParser,
		UnaryOperator<@Nullable R> nextRevisionSupplier,
		String prefix,
		String suffix,
		@RegExp String revisionPattern
	) {
		return new FileNamingSchemeImpl<>(revisionParser, nextRevisionSupplier, prefix, suffix, revisionPattern);
	}

	public static <R extends Comparable<R>> FileNamingScheme<R> of(
		RevisionParser<R> revisionParser,
		UnaryOperator<@Nullable R> nextRevisionSupplier,
		String prefix,
		String suffix,
		@RegExp String revisionPattern,
		String diffPrefix,
		String diffSuffix,
		String diffSeparator
	) {
		return new FileNamingSchemeImpl<>(revisionParser, nextRevisionSupplier, prefix, suffix, revisionPattern, diffPrefix, diffSuffix, diffSeparator);
	}

	@SuppressWarnings("Convert2Lambda")
	public interface RevisionParser<R extends Comparable<R>> {
		R parse(String string) throws IOException;

		static RevisionParser<Long> ofLong() {
			return new RevisionParser<>() {
				@Override
				public Long parse(String string) throws IOException {
					try {
						return Long.parseLong(string);
					} catch (NumberFormatException e) {
						throw new IOException(e);
					}
				}
			};
		}

		static RevisionParser<Integer> ofInteger() {
			return new RevisionParser<>() {
				@Override
				public Integer parse(String string) throws IOException {
					try {
						return Integer.parseInt(string);
					} catch (NumberFormatException e) {
						throw new IOException(e);
					}
				}
			};
		}
	}

	static class FileNamingSchemeImpl<R extends Comparable<R>> implements FileNamingScheme<R> {
		private final RevisionParser<R> revisionParser;
		private final UnaryOperator<@Nullable R> nextRevisionSupplier;

		private final String prefix;
		private final String suffix;
		private final Pattern snapshotPattern;

		private final String diffPrefix;
		private final String diffSuffix;
		private final String diffSeparator;
		private final Pattern diffPattern;

		public FileNamingSchemeImpl(RevisionParser<R> revisionParser, UnaryOperator<@Nullable R> nextRevisionSupplier,
			String prefix, String suffix, @RegExp String revisionPattern
		) {
			this(revisionParser, nextRevisionSupplier, prefix, suffix, revisionPattern, null, null, null);
		}

		public FileNamingSchemeImpl(RevisionParser<R> revisionParser, UnaryOperator<@Nullable R> nextRevisionSupplier,
			String prefix, String suffix, @RegExp String revisionPattern,
			String diffPrefix, String diffSuffix, String diffSeparator
		) {
			this.revisionParser = revisionParser;
			this.nextRevisionSupplier = nextRevisionSupplier;

			this.prefix = prefix;
			this.suffix = suffix;
			this.snapshotPattern = Pattern.compile(Pattern.quote(prefix) + revisionPattern + Pattern.quote(suffix));

			this.diffPrefix = diffPrefix;
			this.diffSuffix = diffSuffix;
			this.diffSeparator = diffSeparator;
			this.diffPattern = diffPrefix != null && diffSuffix != null && diffSeparator != null ?
				Pattern.compile(Pattern.quote(diffPrefix) + revisionPattern + Pattern.quote(diffSeparator) + revisionPattern + Pattern.quote(diffSuffix)) :
				null;
		}

		@Override
		public boolean hasDiffsSupport() {
			return diffPattern != null;
		}

		@Override
		public R nextRevision(@Nullable R previousRevision) {
			return nextRevisionSupplier.apply(previousRevision);
		}

		@Override
		public Pattern snapshotPattern() {
			return snapshotPattern;
		}

		@Override
		public String snapshotPrefix() {
			return prefix;
		}

		@Override
		public String encodeSnapshot(R revision) {
			return prefix + revision + suffix;
		}

		@Override
		public @Nullable R decodeSnapshot(String filename) throws IOException {
			if (!filename.startsWith(prefix)) return null;
			if (!filename.endsWith(suffix)) return null;
			String substring = filename.substring(prefix.length(), filename.length() - suffix.length());
			return revisionParser.parse(substring);
		}

		@Override
		public Pattern diffPattern() {
			if (!hasDiffsSupport()) throw new UnsupportedOperationException();
			return diffPattern;
		}

		@Override
		public String diffPrefix() {
			if (!hasDiffsSupport()) throw new UnsupportedOperationException();
			return diffPrefix;
		}

		@Override
		public String encodeDiff(R from, R to) {
			if (!hasDiffsSupport()) throw new UnsupportedOperationException();
			return diffPrefix + from + diffSeparator + to + diffSuffix;
		}

		@Override
		public Diff<R> decodeDiff(String filename) throws IOException {
			if (!hasDiffsSupport()) throw new UnsupportedOperationException();
			if (!filename.startsWith(diffPrefix)) return null;
			if (!filename.endsWith(diffSuffix)) return null;
			String substring = filename.substring(diffPrefix.length(), filename.length() - diffSuffix.length());
			int pos = substring.indexOf(diffSeparator);
			if (pos == -1) return null;
			String substringFrom = substring.substring(0, pos);
			String substringTo = substring.substring(pos + diffSeparator.length());
			R revisionFrom = revisionParser.parse(substringFrom);
			R revisionTo = revisionParser.parse(substringTo);
			return new Diff<>(revisionFrom, revisionTo);
		}
	}
}
