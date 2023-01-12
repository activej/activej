package io.activej.fs.exception;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.MalformedDataException;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

import static io.activej.fs.util.JsonUtils.fromJson;
import static io.activej.fs.util.JsonUtils.toJson;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class FileSystemExceptionCodecTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testFileSystemException() {
		doTest(new FileSystemException("Test"));
	}

	@Test
	public void testScalarException() {
		doTest(new FileSystemException("Test"));
	}

	@Test
	public void testFileNotFoundException() {
		doTest(new FileNotFoundException("Test"));
	}

	@Test
	public void testFileSystemIOException() {
		doTest(new FileSystemIOException("Test"));
	}

	@Test
	public void testBatchException() {
		doTest(new FileSystemBatchException(Map.of(
				"file1", new FileSystemScalarException("Test"),
				"file2", new FileNotFoundException("Test"),
				"file3", new IsADirectoryException("Test")
		)));
	}

	private static void doTest(FileSystemException exception) {
		ByteBuf json = toJson(FileSystemException.class, exception);
		FileSystemException deserializedException = deserialize(json);

		doAssert(exception, deserializedException);
		if (exception instanceof FileSystemBatchException) {
			Map<String, FileSystemScalarException> exceptions = ((FileSystemBatchException) exception).getExceptions();
			Map<String, FileSystemScalarException> deserializedExceptions = ((FileSystemBatchException) deserializedException).getExceptions();
			for (Map.Entry<String, FileSystemScalarException> entry : exceptions.entrySet()) {
				doAssert(entry.getValue(), deserializedExceptions.get(entry.getKey()));
			}
		}
	}

	private static void doAssert(FileSystemException exception, FileSystemException deserializedException) {
		assertTrue(exception.getStackTrace().length > 0);
		assertEquals(0, deserializedException.getStackTrace().length);
		assertEquals(exception.getClass(), deserializedException.getClass());
		assertEquals(exception.getMessage(), deserializedException.getMessage());
	}

	private static FileSystemException deserialize(ByteBuf json) {
		FileSystemException deserializedException;
		try {
			deserializedException = fromJson(FileSystemException.class, json);
		} catch (MalformedDataException e) {
			throw new AssertionError(e);
		}
		return deserializedException;
	}
}
