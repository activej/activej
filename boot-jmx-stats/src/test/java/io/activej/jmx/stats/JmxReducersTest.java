package io.activej.jmx.stats;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.activej.jmx.api.attribute.JmxReducers.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings("ConstantConditions")
public class JmxReducersTest {

	@Test
	public void distinctReducerReturnsCommonValueIfAllValuesAreSame() {
		JmxReducerDistinct reducer = new JmxReducerDistinct();

		List<String> input = asList("data", "data", "data");

		assertEquals("data", reducer.reduce(input));
	}

	@Test
	public void distinctReducerReturnsNullIfThereAreAtLeastTwoDifferentValuesInInputList() {
		JmxReducerDistinct reducer = new JmxReducerDistinct();

		List<String> input = asList("data", "non-data", "data");

		assertNull(reducer.reduce(input));
	}

	@Test
	public void sumReducerWorksCorrectlyWithIntegerNumbers() {
		JmxReducerSum sumReducer = new JmxReducerSum();
		List<Integer> numbers = new ArrayList<>();
		numbers.add(10);
		numbers.add(15);

		int result = (int) sumReducer.reduce(numbers);
		assertEquals(25, result);
	}

	@Test
	public void sumReducerWorksCorrectlyWithFloatingPointNumbers() {
		JmxReducerSum sumReducer = new JmxReducerSum();
		List<Double> numbers = new ArrayList<>();
		numbers.add(5.0);
		numbers.add(2.5);

		double result = (double) sumReducer.reduce(numbers);
		double acceptableError = 10E-3;
		assertEquals(7.5, result, acceptableError);
	}

	@Test
	public void sumReducerIgnoresNullValues() {
		JmxReducerSum sumReducer = new JmxReducerSum();
		List<Integer> numbers = new ArrayList<>();
		numbers.add(10);
		numbers.add(null);
		numbers.add(15);

		int result = (int) sumReducer.reduce(numbers);
		assertEquals(25, result);
	}

	@Test
	public void sumReducerReturnsNullInCaseOfEmptyList() {
		JmxReducerSum sumReducer = new JmxReducerSum();
		List<Number> numbers = new ArrayList<>();

		assertNull(sumReducer.reduce(numbers));
	}

	@Test
	public void minReducerWorksCorrectlyWithFloatingPointNumbers() {
		JmxReducerMin minReducer = new JmxReducerMin();
		List<Number> numbers = new ArrayList<>();
		numbers.add(5.0);
		numbers.add(2.5);
		numbers.add(10.0);

		double result = (double) minReducer.reduce(numbers);
		double acceptableError = 10E-3;
		assertEquals(2.5, result, acceptableError);
	}

	@Test
	public void minReducerWorksCorrectlyWithIntegerNumbers() {
		JmxReducerMin minReducer = new JmxReducerMin();
		List<Integer> numbers = new ArrayList<>();
		numbers.add(5);
		numbers.add(2);
		numbers.add(10);

		int result = (int) minReducer.reduce(numbers);
		assertEquals(2, result);
	}

	@Test
	public void minReducerReturnsNullInCaseOfEmptyList() {
		JmxReducerMin minReducer = new JmxReducerMin();
		List<Number> numbers = new ArrayList<>();

		assertNull(minReducer.reduce(numbers));
	}

	@Test
	public void maxReducerWorksCorrectlyWithFloatingPointNumbers() {
		JmxReducerMax maxReducer = new JmxReducerMax();
		List<Double> numbers = new ArrayList<>();
		numbers.add(5.0);
		numbers.add(2.5);
		numbers.add(10.0);

		double result = (double) maxReducer.reduce(numbers);
		double acceptableError = 10E-3;
		assertEquals(10.0, result, acceptableError);
	}

	@Test
	public void maxReducerWorksCorrectlyWithIntegerNumbers() {
		JmxReducerMax maxReducer = new JmxReducerMax();
		List<Long> numbers = new ArrayList<>();
		numbers.add(5L);
		numbers.add(2L);
		numbers.add(10L);

		long result = (long) maxReducer.reduce(numbers);
		assertEquals(10L, result);
	}

	@Test
	public void maxReducerReturnsNullInCaseOfEmptyList() {
		JmxReducerMin maxReducer = new JmxReducerMin();
		List<Number> numbers = new ArrayList<>();

		assertNull(maxReducer.reduce(numbers));
	}
}
