package io.activej.common;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertArrayEquals;

public class UtilsTest {
	private static byte[] expected;

	@BeforeClass
	public static void init() throws IOException {
		expected = Files.readAllBytes(Paths.get("src/test/resources/dir/testResource.txt"));
	}

	@Test
	public void loadResourceString() throws IOException {
		byte[] actual = Utils.loadResource("dir/testResource.txt");
		assertArrayEquals(expected, actual);
	}

	@Test
	public void loadResourcePath() throws IOException {
		byte[] actual = Utils.loadResource(Paths.get("dir/testResource.txt"));
		assertArrayEquals(expected, actual);
	}
}
