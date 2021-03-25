package io.activej.state.file;

import org.jetbrains.annotations.Nullable;

public class FileNamingSchemes {

	public static FileNamingScheme create(String prefix, String suffix) {
		return new FileNamingSchemeImpl(prefix, suffix);
	}

	public static FileNamingScheme create(String prefix, String suffix, String diffPrefix, String diffSuffix, char diffSeparator) {
		return new FileNamingSchemeImplImpl2(prefix, suffix, diffPrefix, diffSuffix, diffSeparator);
	}

	private static class FileNamingSchemeImpl implements FileNamingScheme {
		private final String prefix;
		private final String suffix;

		public FileNamingSchemeImpl(String prefix, String suffix) {
			this.prefix = prefix;
			this.suffix = suffix;
		}

		@Override
		public String snapshotGlob() {
			return prefix + "*" + suffix;
		}

		@Override
		public String encodeSnapshot(long revision) {
			return prefix + revision + suffix;
		}

		@Override
		@Nullable
		public Long decodeSnapshot(String filename) {
			if (!filename.startsWith(prefix)) return null;
			if (!filename.endsWith(suffix)) return null;
			String substring = filename.substring(prefix.length(), filename.length() - suffix.length());
			try {
				return Long.parseLong(substring);
			} catch (NumberFormatException e) {
				return null;
			}
		}

		@Override
		public String diffGlob() {
			throw new UnsupportedOperationException();
		}

		@Override
		public String diffGlob(long from) {
			throw new UnsupportedOperationException();
		}

		@Override
		public String encodeDiff(long from, long to) {
			throw new UnsupportedOperationException();
		}

		@Override
		public @Nullable Diff decodeDiff(String filename) {
			throw new UnsupportedOperationException();
		}
	}

	private static class FileNamingSchemeImplImpl2 extends FileNamingSchemeImpl {
		private final String diffPrefix;
		private final String diffSuffix;
		private final char diffSeparator;

		public FileNamingSchemeImplImpl2(String prefix, String suffix, String diffPrefix, String diffSuffix, char diffSeparator) {
			super(prefix, suffix);
			this.diffPrefix = diffPrefix;
			this.diffSuffix = diffSuffix;
			this.diffSeparator = diffSeparator;
		}

		@Override
		public String diffGlob() {
			return diffPrefix + "*" + diffSuffix;
		}

		@Override
		public String diffGlob(long from) {
			return diffPrefix + from + diffSeparator + "*";
		}

		@Override
		public String encodeDiff(long from, long to) {
			return diffPrefix + from + diffSeparator + to + diffSuffix;
		}

		@Override
		@Nullable
		public Diff decodeDiff(String filename) {
			if (!filename.startsWith(diffPrefix)) return null;
			if (!filename.endsWith(diffSuffix)) return null;
			String substring = filename.substring(diffPrefix.length(), filename.length() - diffSuffix.length());
			int pos = substring.indexOf(diffSeparator);
			if (pos == -1) return null;
			String substringFrom = substring.substring(0, pos);
			String substringTo = substring.substring(pos + 1);
			try {
				long revisionFrom = Long.parseLong(substringFrom);
				long revisionTo = Long.parseLong(substringTo);
				return new Diff(revisionFrom, revisionTo);
			} catch (NumberFormatException e) {
				return null;
			}
		}
	}
}
