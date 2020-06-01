package io.activej.http.decoder;

import org.junit.Test;

import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class ValidatorTest {
	@Test
	public void testAnd() {
		Validator<String> validator =
				Validator.of(s -> !s.isEmpty(), "test");
		validator = validator.and(Validator.of(s -> s.length() > 5, "Invalid length"));

		List<DecodeError> errors = validator.validate("");
		assertEquals(2, errors.size());

		errors = validator.validate("tmp");
		assertEquals(1, errors.size());
	}

	@Test(expected = NullPointerException.class)
	public void testAndNull() {
		Validator<String> validator =
				Validator.of(Objects::nonNull, "test");
		validator = validator.and(Validator.of(s -> s.length() > 5, "Invalid length"));

		validator.validate(null);
	}

	@Test
	public void testThenNull() {
		Validator<String> validator =
				Validator.of(Objects::nonNull, "Cannot be null");
		validator = validator.then(Validator.of(s -> s.length() > 5, "Invalid length"));

		assertEquals(validator.validate(null).get(0).getMessage(), "Cannot be null");
	}
}
