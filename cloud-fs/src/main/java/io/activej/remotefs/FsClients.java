package io.activej.remotefs;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.remotefs.util.RemoteFsUtils.escapeGlob;

public class FsClients {
	public static FsClient empty() {
		return EmptyFsClient.INSTANCE;
	}

	public static FsClient transform(FsClient originalClient, Function<String, Optional<String>> into, Function<String, Optional<String>> from, Function<String, Optional<String>> globInto) {
		return new TransformFsClient(originalClient, into, from, globInto);
	}

	public static FsClient transform(FsClient originalClient, Function<String, Optional<String>> into, Function<String, Optional<String>> from) {
		return transform(originalClient, into, from, $ -> Optional.empty());
	}

	// similar to 'chroot'
	public static FsClient addPrefix(FsClient originalClient, String prefix) {
		if (prefix.length() == 0) {
			return originalClient;
		}
		String escapedPrefix = escapeGlob(prefix);
		return transform(originalClient,
				name -> Optional.of(prefix + name),
				name -> Optional.ofNullable(name.startsWith(prefix) ? name.substring(prefix.length()) : null),
				name -> Optional.of(escapedPrefix + name)
		);
	}

	// similar to 'cd'
	public static FsClient subdirectory(FsClient originalClient, String dir) {
		if (dir.length() == 0) {
			return originalClient;
		}
		return addPrefix(originalClient, dir.endsWith("/") ? dir : dir + '/');
	}

	public static FsClient removePrefix(FsClient originalClient, String prefix) {
		if (prefix.length() == 0) {
			return originalClient;
		}
		String escapedPrefix = escapeGlob(prefix);
		return transform(originalClient,
				name -> Optional.ofNullable(name.startsWith(prefix) ? name.substring(prefix.length()) : null),
				name -> Optional.of(prefix + name),
				name -> Optional.of(name.startsWith(escapedPrefix) ? name.substring(escapedPrefix.length()) : "**")
		);
	}

	public static FsClient filter(FsClient originalClient, Predicate<String> predicate) {
		return new FilterFsClient(originalClient, predicate);
	}

	public static FsClient mount(FsClient root, Map<String, FsClient> mounts) {
		return new MountingFsClient(root,
				mounts.entrySet().stream()
						.collect(Collectors.toMap(
								Map.Entry::getKey,
								e -> removePrefix(e.getValue(), e.getKey()))));
	}

}
