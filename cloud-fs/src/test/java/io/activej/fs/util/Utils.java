package io.activej.fs.util;

import io.activej.fs.exception.FsBatchException;
import io.activej.fs.exception.FsScalarException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.activej.common.collection.CollectionUtils.map;
import static io.activej.fs.LocalActiveFs.DEFAULT_TEMP_DIR;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

public final class Utils {

	public static void initTempDir(Path storage) {
		try {
			Files.createDirectories(storage.resolve(DEFAULT_TEMP_DIR));
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	public static void assertBatchException(@NotNull Throwable e, String name, Class<? extends FsScalarException> exceptionClass) {
		assertBatchException(e, map(name, exceptionClass));
	}

	public static void assertBatchException(@NotNull Throwable e, Map<String, Class<? extends FsScalarException>> exceptionClasses) {
		assertThat(e, instanceOf(FsBatchException.class));
		FsBatchException batchEx = (FsBatchException) e;

		Map<String, FsScalarException> exceptions = batchEx.getExceptions();

		for (Map.Entry<String, Class<? extends FsScalarException>> entry : exceptionClasses.entrySet()) {
			assertThat(exceptions.get(entry.getKey()), instanceOf(entry.getValue()));
		}
	}

}
