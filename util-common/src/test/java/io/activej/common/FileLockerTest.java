package io.activej.common;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileLockerTest {

	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	private String lockFile;

	@Before
	public void setUp() throws Exception {
		lockFile = Paths.get(folder.newFolder().getPath()).resolve(".lock").toString();
	}

	@Test
	public void lockAndReleaseFile() {
		FileLocker fileLocker = new FileLocker(lockFile);

		assertFalse(fileLocker.isLocked());
		assertTrue(fileLocker.obtainLock());
		assertTrue(fileLocker.isLocked());

		fileLocker.releaseLock();
		assertFalse(fileLocker.isLocked());
	}

	@Test
	public void twoFileLockers() {
		FileLocker fileLocker1 = new FileLocker(lockFile);
		assertFalse(fileLocker1.isLocked());
		assertTrue(fileLocker1.obtainLock());
		assertTrue(fileLocker1.isLocked());

		FileLocker fileLocker2 = new FileLocker(lockFile);
		assertFalse(fileLocker2.isLocked());
		assertFalse(fileLocker2.obtainLock());

		assertTrue(fileLocker1.isLocked());
		assertFalse(fileLocker2.isLocked());

		fileLocker1.releaseLock();
		assertFalse(fileLocker1.isLocked());
		assertTrue(fileLocker2.obtainLock());
		assertTrue(fileLocker2.isLocked());

		fileLocker2.releaseLock();
	}

	@Test
	public void closeFileLocker() throws IOException {
		FileLocker fileLocker1 = new FileLocker(lockFile);
		fileLocker1.obtainLock();

		FileLocker fileLocker2 = new FileLocker(lockFile);
		assertFalse(fileLocker2.obtainLock());

		assertTrue(fileLocker1.isLocked());
		assertFalse(fileLocker2.isLocked());

		fileLocker1.close();
		assertTrue(fileLocker2.obtainLock());
		assertTrue(fileLocker2.isLocked());

		fileLocker2.close();

		assertFalse(fileLocker1.isLocked());
		assertFalse(fileLocker2.isLocked());
	}

	@Test
	public void fileLockerTryWithResources() throws IOException {
		FileLocker fileLocker2 = new FileLocker(lockFile);
		try(FileLocker fileLocker1 = new FileLocker(lockFile)){
			fileLocker1.obtainLock();

			assertFalse(fileLocker2.obtainLock());

			assertTrue(fileLocker1.isLocked());
			assertFalse(fileLocker2.isLocked());
		}
		assertTrue(fileLocker2.obtainLock());
		assertTrue(fileLocker2.isLocked());

		fileLocker2.close();

		assertFalse(fileLocker2.isLocked());
	}
}
