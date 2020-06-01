package io.activej.promise;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

/**
 * @since 3.0.0
 */
@State(Scope.Benchmark)
public class DkPromiseBenchmark {

	@Benchmark
	public void oneCallMeasure(Blackhole blackhole) {
		SettablePromise<Integer> promise = new SettablePromise<>();
		promise.map(a -> a + a).whenComplete((result, e) -> blackhole.consume(result));
		promise.set(10);
	}

	@Benchmark
	public void combineMeasure(Blackhole blackhole) {
		SettablePromise<Integer> promise = new SettablePromise<>();
		SettablePromise<Integer> additional = new SettablePromise<>();
		promise.map(a -> a + a).then(a -> additional).whenComplete((result, e) -> blackhole.consume(result));
		promise.set(10);
		additional.set(10);
	}

	public static void main(String[] args) throws RunnerException {

		Options opt = new OptionsBuilder()
				.include(DkPromiseBenchmark.class.getSimpleName())
				.forks(2)
				.warmupIterations(3)
				.warmupTime(TimeValue.seconds(1L))
				.measurementIterations(5)
				.measurementTime(TimeValue.seconds(2L))
				.mode(Mode.AverageTime)
				.timeUnit(TimeUnit.NANOSECONDS)
				.build();

		new Runner(opt).run();
	}
}


