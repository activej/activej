package io.activej.fs.exception;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.MalformedDataException;
import io.activej.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;

import static io.activej.common.Utils.mapOf;
import static io.activej.fs.util.RemoteFsUtils.fromJson;
import static io.activej.fs.util.RemoteFsUtils.toJson;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class FsExceptionCodecTest {

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testFsException() {
		doTest(new FsException("Test"));
	}

	@Test
	public void testScalarException() {
		doTest(new FsException("Test"));
	}

	@Test
	public void testFileNotFoundException() {
		doTest(new FileNotFoundException("Test"));
	}

	@Test
	public void testFsIoException() {
		doTest(new FsIOException("Test"));
	}

	@Test
	public void testBatchException() {
		doTest(new FsBatchException(mapOf(
				"file1", new FsScalarException("Test"),
				"file2", new FileNotFoundException("Test"),
				"file3", new IsADirectoryException("Test")
		)));
	}

	private static void doTest(FsException exception) {
		ByteBuf json = toJson(FsException.class, exception);
		FsException deserializedException = deserialize(json);

		doAssert(exception, deserializedException);
		if (exception instanceof FsBatchException) {
			Map<String, FsScalarException> exceptions = ((FsBatchException) exception).getExceptions();
			Map<String, FsScalarException> deserializedExceptions = ((FsBatchException) deserializedException).getExceptions();
			for (Map.Entry<String, FsScalarException> entry : exceptions.entrySet()) {
				doAssert(entry.getValue(), deserializedExceptions.get(entry.getKey()));
			}
		}
	}

	private static void doAssert(FsException exception, FsException deserializedException) {
		assertTrue(exception.getStackTrace().length > 0);
		assertEquals(0, deserializedException.getStackTrace().length);
		assertEquals(exception.getClass(), deserializedException.getClass());
		assertEquals(exception.getMessage(), deserializedException.getMessage());
	}

	private static FsException deserialize(ByteBuf json) {
		FsException deserializedException;
		try {
			deserializedException = fromJson(FsException.class, json);
		} catch (MalformedDataException e) {
			throw new AssertionError(e);
		}
		return deserializedException;
	}
}
