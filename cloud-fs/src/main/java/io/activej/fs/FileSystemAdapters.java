package io.activej.fs;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.fs.util.RemoteFileSystemUtils.escapeGlob;

public class FileSystemAdapters {

	public static AsyncFileSystem transform(AsyncFileSystem originalFS, Function<String, Optional<String>> into, Function<String, Optional<String>> from, Function<String, Optional<String>> globInto) {
		return new FileSystem_Transform(originalFS, into, from, globInto);
	}

	public static AsyncFileSystem transform(AsyncFileSystem originalFS, Function<String, Optional<String>> into, Function<String, Optional<String>> from) {
		return transform(originalFS, into, from, $ -> Optional.empty());
	}

	// similar to 'chroot'
	public static AsyncFileSystem addPrefix(AsyncFileSystem originalFS, String prefix) {
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
	public static AsyncFileSystem subdirectory(AsyncFileSystem originalFS, String dir) {
		if (dir.length() == 0) {
			return originalFS;
		}
		return addPrefix(originalFS, dir.endsWith("/") ? dir : dir + '/');
	}

	public static AsyncFileSystem removePrefix(AsyncFileSystem originalFS, String prefix) {
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

	public static AsyncFileSystem filter(AsyncFileSystem originalFS, Predicate<String> predicate) {
		return new FileSystem_Filter(originalFS, predicate);
	}

	public static AsyncFileSystem mount(AsyncFileSystem root, Map<String, AsyncFileSystem> mounts) {
		return new FileSystem_Mounting(root,
				mounts.entrySet().stream()
						.collect(Collectors.toMap(
								Map.Entry::getKey,
								e -> removePrefix(e.getValue(), e.getKey()))));
	}

}
