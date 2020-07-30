package io.activej.fs.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.activej.fs.LocalActiveFs.DEFAULT_TEMP_DIR;

public final class Utils {

	public static void initTempDir(Path storage){
		try {
			Files.createDirectories(storage.resolve(DEFAULT_TEMP_DIR));
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

}
