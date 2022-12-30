package io.activej.fs;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.fs.util.RemoteFsUtils.escapeGlob;

public class ActiveFsAdapters {

	public static IActiveFs transform(IActiveFs originalFs, Function<String, Optional<String>> into, Function<String, Optional<String>> from, Function<String, Optional<String>> globInto) {
		return new TransformActiveFs(originalFs, into, from, globInto);
	}

	public static IActiveFs transform(IActiveFs originalFs, Function<String, Optional<String>> into, Function<String, Optional<String>> from) {
		return transform(originalFs, into, from, $ -> Optional.empty());
	}

	// similar to 'chroot'
	public static IActiveFs addPrefix(IActiveFs originalFs, String prefix) {
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
	public static IActiveFs subdirectory(IActiveFs originalFs, String dir) {
		if (dir.length() == 0) {
			return originalFs;
		}
		return addPrefix(originalFs, dir.endsWith("/") ? dir : dir + '/');
	}

	public static IActiveFs removePrefix(IActiveFs originalFs, String prefix) {
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

	public static IActiveFs filter(IActiveFs originalFs, Predicate<String> predicate) {
		return new FilterActiveFs(originalFs, predicate);
	}

	public static IActiveFs mount(IActiveFs root, Map<String, IActiveFs> mounts) {
		return new MountingActiveFs(root,
				mounts.entrySet().stream()
						.collect(Collectors.toMap(
								Map.Entry::getKey,
								e -> removePrefix(e.getValue(), e.getKey()))));
	}

}
