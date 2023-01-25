package io.activej.fs;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.fs.util.RemoteFileSystemUtils.escapeGlob;

public class FileSystemAdapters {

	public static IFileSystem transform(IFileSystem originalFS, Function<String, Optional<String>> into, Function<String, Optional<String>> from, Function<String, Optional<String>> globInto) {
		return new FileSystem_Transform(originalFS, into, from, globInto);
	}

	public static IFileSystem transform(IFileSystem originalFS, Function<String, Optional<String>> into, Function<String, Optional<String>> from) {
		return transform(originalFS, into, from, $ -> Optional.empty());
	}

	// similar to 'chroot'
	public static IFileSystem addPrefix(IFileSystem originalFS, String prefix) {
		if (prefix.length() == 0) {
			return originalFS;
		}
		String escapedPrefix = escapeGlob(prefix);
		return transform(originalFS,
				name -> Optional.of(prefix + name),
				name -> Optional.ofNullable(name.startsWith(prefix) ? name.substring(prefix.length()) : null),
				name -> Optional.of(escapedPrefix + name)
		);
	}

	// similar to 'cd'
	public static IFileSystem subdirectory(IFileSystem originalFS, String dir) {
		if (dir.length() == 0) {
			return originalFS;
		}
		return addPrefix(originalFS, dir.endsWith("/") ? dir : dir + '/');
	}

	public static IFileSystem removePrefix(IFileSystem originalFS, String prefix) {
		if (prefix.length() == 0) {
			return originalFS;
		}
		String escapedPrefix = escapeGlob(prefix);
		return transform(originalFS,
				name -> Optional.ofNullable(name.startsWith(prefix) ? name.substring(prefix.length()) : null),
				name -> Optional.of(prefix + name),
				name -> Optional.of(name.startsWith(escapedPrefix) ? name.substring(escapedPrefix.length()) : "**")
		);
	}

	public static IFileSystem filter(IFileSystem originalFS, Predicate<String> predicate) {
		return new FileSystem_Filter(originalFS, predicate);
	}

	public static IFileSystem mount(IFileSystem root, Map<String, IFileSystem> mounts) {
		return new FileSystem_Mounting(root,
				mounts.entrySet().stream()
						.collect(Collectors.toMap(
								Map.Entry::getKey,
								e -> removePrefix(e.getValue(), e.getKey()))));
	}

}
