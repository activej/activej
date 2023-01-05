package io.activej.fs;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.fs.util.RemoteFsUtils.escapeGlob;

public class FsAdapters {

	public static AsyncFs transform(AsyncFs originalFs, Function<String, Optional<String>> into, Function<String, Optional<String>> from, Function<String, Optional<String>> globInto) {
		return new TransformFs(originalFs, into, from, globInto);
	}

	public static AsyncFs transform(AsyncFs originalFs, Function<String, Optional<String>> into, Function<String, Optional<String>> from) {
		return transform(originalFs, into, from, $ -> Optional.empty());
	}

	// similar to 'chroot'
	public static AsyncFs addPrefix(AsyncFs originalFs, String prefix) {
		if (prefix.length() == 0) {
			return originalFs;
		}
		String escapedPrefix = escapeGlob(prefix);
		return transform(originalFs,
				name -> Optional.of(prefix + name),
				name -> Optional.ofNullable(name.startsWith(prefix) ? name.substring(prefix.length()) : null),
				name -> Optional.of(escapedPrefix + name)
		);
	}

	// similar to 'cd'
	public static AsyncFs subdirectory(AsyncFs originalFs, String dir) {
		if (dir.length() == 0) {
			return originalFs;
		}
		return addPrefix(originalFs, dir.endsWith("/") ? dir : dir + '/');
	}

	public static AsyncFs removePrefix(AsyncFs originalFs, String prefix) {
		if (prefix.length() == 0) {
			return originalFs;
		}
		String escapedPrefix = escapeGlob(prefix);
		return transform(originalFs,
				name -> Optional.ofNullable(name.startsWith(prefix) ? name.substring(prefix.length()) : null),
				name -> Optional.of(prefix + name),
				name -> Optional.of(name.startsWith(escapedPrefix) ? name.substring(escapedPrefix.length()) : "**")
		);
	}

	public static AsyncFs filter(AsyncFs originalFs, Predicate<String> predicate) {
		return new FilterFs(originalFs, predicate);
	}

	public static AsyncFs mount(AsyncFs root, Map<String, AsyncFs> mounts) {
		return new MountingFs(root,
				mounts.entrySet().stream()
						.collect(Collectors.toMap(
								Map.Entry::getKey,
								e -> removePrefix(e.getValue(), e.getKey()))));
	}

}
