package io.activej.http.decoder;

import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.activej.common.Utils.listOf;
import static org.junit.Assert.*;

public class DecodeErrorsTest {
	@Test
	public void testMergeWithTheSame() {
		DecodeErrors tree = DecodeErrors.create();
		tree.with(listOf(DecodeError.of("test1"),
				DecodeError.of("test2"),
				DecodeError.of("test3"),
				DecodeError.of("test4")));

		DecodeErrors tree2 = DecodeErrors.create();
		tree.with(listOf(DecodeError.of("test11"),
				DecodeError.of("test22"),
				DecodeError.of("test33"),
				DecodeError.of("test44")));

		tree.merge(tree2);
		assertEquals(8, tree.toMultimap().get("").size());
	}

	@Test
	public void test() {
		DecodeErrors tree = DecodeErrors.of("Test");
		assertTrue(tree.hasErrors());

		assertEquals(Collections.emptySet(), tree.getChildren());
		assertEquals(1, tree.toMap().size());
		assertNull(tree.getChild("$"));
		assertEquals(1, tree.toMap().size());
	}

	@Test
	public void testMap() {
		DecodeErrors tree = DecodeErrors.create();
		tree.with("test", DecodeErrors.of("tmp1")
				.with("test2", DecodeErrors.of("tmp2")));
		tree.with("test3", DecodeErrors.of("tmp3"));
		Map<String, String> errors = tree.toMap();
		assertEquals(3, errors.size());
		assertNotNull(errors.get("test"));
		assertNotNull(errors.get("test.test2"));
		assertNotNull(errors.get("test3"));

		Map<String, String> errorsWithSeparator = tree.toMap("-");
		assertNotNull(errorsWithSeparator.get("test-test2"));
	}

	@Test
	public void testMultiMap() {
		DecodeErrors tree = DecodeErrors.create();
		tree.with("test", DecodeErrors.of("tmp1")
				.with("test2", DecodeErrors.of("tmp2")));
		tree.with("test3", DecodeErrors.of("tmp3"));

		Map<String, List<String>> errors = tree.toMultimap();
		assertEquals(3, errors.size());
		assertEquals(1, errors.get("test").size());
	}
}
