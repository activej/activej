package io.activej.fs.adapter;

import io.activej.common.annotation.StaticFactories;
import io.activej.fs.IFileSystem;
import io.activej.fs.adapter.impl.Filter;
import io.activej.fs.adapter.impl.Mounting;
import io.activej.fs.adapter.impl.Transform;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.fs.util.RemoteFileSystemUtils.escapeGlob;

@StaticFactories(IFileSystem.class)
public class FileSystemAdapters {

	/**
	 * Creates a file system adapter that can transform paths and filename via mapping functions.
	 * <p>
	 * Inherits all the limitations of parent {@link IFileSystem}
	 */
	public static IFileSystem transform(IFileSystem originalFS, Function<String, Optional<String>> into, Function<String, Optional<String>> from, Function<String, Optional<String>> globInto) {
		return new Transform(originalFS, into, from, globInto);
	}

	/**
	 * @see #transform(IFileSystem, Function, Function, Function)
	 */
	public static IFileSystem transform(IFileSystem originalFS, Function<String, Optional<String>> into, Function<String, Optional<String>> from) {
		return transform(originalFS, into, from, $ -> Optional.empty());
	}

	// similar to 'chroot'
	public static IFileSystem addPrefix(IFileSystem originalFS, String prefix) {
		if (prefix.isEmpty()) {
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
		if (dir.isEmpty()) {
			return originalFS;
		}
		return addPrefix(originalFS, dir.endsWith("/") ? dir : dir + '/');
	}

	public static IFileSystem removePrefix(IFileSystem originalFS, String prefix) {
		if (prefix.isEmpty()) {
			return originalFS;
		}
		String escapedPrefix = escapeGlob(prefix);
		return transform(originalFS,
			name -> Optional.ofNullable(name.startsWith(prefix) ? name.substring(prefix.length()) : null),
			name -> Optional.of(prefix + name),
			name -> Optional.of(name.startsWith(escapedPrefix) ? name.substring(escapedPrefix.length()) : "**")
		);
	}

	/**
	 * Creates a file system that can be configured to forbid certain paths and filenames.
	 * <p>
	 * Inherits all the limitations of parent {@link IFileSystem}
	 */
	public static IFileSystem filter(IFileSystem originalFS, Predicate<String> predicate) {
		return new Filter(originalFS, predicate);
	}

	/**
	 * Creates a file system that allows to mount several {@link IFileSystem} implementations to correspond to different filenames.
	 * <p>
	 * Inherits the most strict limitations of all the mounted file systems implementations and root file system.
	 */
	public static IFileSystem mount(IFileSystem root, Map<String, IFileSystem> mounts) {
		return new Mounting(root,
			mounts.entrySet().stream()
				.collect(Collectors.toMap(
					Map.Entry::getKey,
					e -> removePrefix(e.getValue(), e.getKey()))));
	}

}
