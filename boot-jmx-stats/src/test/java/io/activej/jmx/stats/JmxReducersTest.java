package io.activej.jmx.stats;

import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.activej.jmx.api.attribute.JmxReducers.*;
import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class JmxReducersTest {

	@Test
	public void distinctReducerReturnsCommonValueIfAllValuesAreSame() {
		JmxReducerDistinct reducer = new JmxReducerDistinct();

		List<String> input = List.of("data", "data", "data");

		assertEquals("data", reducer.reduce(input));
	}

	@Test
	public void distinctReducerReturnsNullIfThereAreAtLeastTwoDifferentValuesInInputList() {
		JmxReducerDistinct reducer = new JmxReducerDistinct();

		List<String> input = List.of("data", "non-data", "data");

		assertNull(reducer.reduce(input));
	}

	@Test
	public void anyReducerReturnsAnyNonNullValue() {
		JmxReducerAny reducer = new JmxReducerAny();

		List<String> input = new ArrayList<>();
		input.add(null);
		input.add("data1");
		input.add("data2");

		Object result = reducer.reduce(input);
		assertTrue("data1".equals(result) || "data2".equals(result));
	}

	@Test
	public void anyReducerReturnsNullIfThereAreNoNonNullValues() {
		JmxReducerAny reducer = new JmxReducerAny();

		List<String> input = new ArrayList<>();
		input.add(null);
		input.add(null);
		input.add(null);

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
	public void sumReducerWorksCorrectlyWithDuration() {
		JmxReducerSum sumReducer = new JmxReducerSum();
		List<Duration> durations = new ArrayList<>();
		durations.add(Duration.ofMinutes(3));
		durations.add(Duration.ofSeconds(35));

		Duration result = (Duration) sumReducer.reduce(durations);
		assertEquals(Duration.ofMinutes(3).plus(Duration.ofSeconds(35)), result);
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
	public void avgReducerWorksCorrectlyWithIntegerNumbers() {
		JmxReducerAvg avgReducer = new JmxReducerAvg();
		List<Integer> numbers = new ArrayList<>();
		numbers.add(10);
		numbers.add(20);

		int result = (int) avgReducer.reduce(numbers);
		assertEquals(15, result);
	}

	@Test
	public void avgReducerWorksCorrectlyWithFloatingPointNumbers() {
		JmxReducerAvg avgReducer = new JmxReducerAvg();
		List<Double> numbers = new ArrayList<>();
		numbers.add(5.0);
		numbers.add(2.5);

		double result = (double) avgReducer.reduce(numbers);
		double acceptableError = 10E-3;
		assertEquals(7.5 / 2, result, acceptableError);
	}

	@Test
	public void avgReducerWorksCorrectlyWithDuration() {
		JmxReducerAvg avgReducer = new JmxReducerAvg();
		List<Duration> durations = new ArrayList<>();
		durations.add(Duration.ofMinutes(3));
		durations.add(Duration.ofSeconds(35));

		Duration result = (Duration) avgReducer.reduce(durations);
		assertEquals(Duration.ofMinutes(3).plus(Duration.ofSeconds(35)).dividedBy(2), result);
	}

	@Test
	public void avgReducerIgnoresNullValues() {
		JmxReducerAvg avgReducer = new JmxReducerAvg();
		List<Integer> numbers = new ArrayList<>();
		numbers.add(10);
		numbers.add(null);
		numbers.add(15);

		int result = (int) avgReducer.reduce(numbers);
		assertEquals(12, result);
	}

	@Test
	public void avgReducerReturnsNullInCaseOfEmptyList() {
		JmxReducerAvg avgReducer = new JmxReducerAvg();
		List<Number> numbers = new ArrayList<>();

		assertNull(avgReducer.reduce(numbers));
	}

	@Test
	public void minReducerWorksCorrectlyWithFloatingPointNumbers() {
		JmxReducerMin<Double> minReducer = new JmxReducerMin<>();
		List<Double> numbers = new ArrayList<>();
		numbers.add(5.0);
		numbers.add(2.5);
		numbers.add(10.0);

		double result = minReducer.reduce(numbers);
		double acceptableError = 10E-3;
		assertEquals(2.5, result, acceptableError);
	}

	@Test
	public void minReducerWorksCorrectlyWithIntegerNumbers() {
		JmxReducerMin<Integer> minReducer = new JmxReducerMin<>();
		List<Integer> numbers = new ArrayList<>();
		numbers.add(5);
		numbers.add(2);
		numbers.add(10);

		int result = minReducer.reduce(numbers);
		assertEquals(2, result);
	}

	@Test
	public void minReducerWorksCorrectlyWithDuration() {
		JmxReducerMin<Duration> minReducer = new JmxReducerMin<>();
		List<Duration> durations = new ArrayList<>();
		durations.add(Duration.ofSeconds(5));
		durations.add(Duration.ofSeconds(2));
		durations.add(Duration.ofSeconds(10));

		Duration result = minReducer.reduce(durations);
		assertEquals(Duration.ofSeconds(2), result);
	}

	@Test
	public void minReducerReturnsNullInCaseOfEmptyList() {
		JmxReducerMin<Integer> minReducer = new JmxReducerMin<>();
		List<Integer> numbers = new ArrayList<>();

		assertNull(minReducer.reduce(numbers));
	}

	@Test
	public void maxReducerWorksCorrectlyWithFloatingPointNumbers() {
		JmxReducerMax<Double> maxReducer = new JmxReducerMax<>();
		List<Double> numbers = new ArrayList<>();
		numbers.add(5.0);
		numbers.add(2.5);
		numbers.add(10.0);

		double result = maxReducer.reduce(numbers);
		double acceptableError = 10E-3;
		assertEquals(10.0, result, acceptableError);
	}

	@Test
	public void maxReducerWorksCorrectlyWithIntegerNumbers() {
		JmxReducerMax<Long> maxReducer = new JmxReducerMax<>();
		List<Long> numbers = new ArrayList<>();
		numbers.add(5L);
		numbers.add(2L);
		numbers.add(10L);

		long result = maxReducer.reduce(numbers);
		assertEquals(10L, result);
	}

	@Test
	public void maxReducerWorksCorrectlyWithDuration() {
		JmxReducerMax<Duration> maxReducer = new JmxReducerMax<>();
		List<Duration> durations = new ArrayList<>();
		durations.add(Duration.ofSeconds(5));
		durations.add(Duration.ofSeconds(2));
		durations.add(Duration.ofSeconds(10));

		Duration result = maxReducer.reduce(durations);
		assertEquals(Duration.ofSeconds(10), result);
	}

	@Test
	public void maxReducerReturnsNullInCaseOfEmptyList() {
		JmxReducerMin<Integer> maxReducer = new JmxReducerMin<>();
		List<Integer> numbers = new ArrayList<>();

		assertNull(maxReducer.reduce(numbers));
	}

	@Test
	public void concatReducerWorksCorrectlyWithScalarValues() {
		JmxReducerConcat concatReducer = new JmxReducerConcat();
		List<Integer> numbers = new ArrayList<>();
		numbers.add(5);
		numbers.add(2);
		numbers.add(10);

		List<String> result = concatReducer.reduce(numbers);
		assertEquals(List.of("5", "2", "10"), result);
	}

	@Test
	public void concatReducerFlattensLists() {
		JmxReducerConcat concatReducer = new JmxReducerConcat();
		List<List<Integer>> numberLists = new ArrayList<>();
		numberLists.add(List.of(1, 2, 3));
		numberLists.add(List.of());
		numberLists.add(List.of(4, 5, 3));

		List<String> result = concatReducer.reduce(numberLists);
		assertEquals(List.of("1", "2", "3", "4", "5", "3"), result);
	}

	@Test
	public void concatReducerReturnsNullInCaseOfEmptyList() {
		JmxReducerConcat concatReducer = new JmxReducerConcat();
		List<Integer> numbers = new ArrayList<>();

		assertNull(concatReducer.reduce(numbers));
	}
}
