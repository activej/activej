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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @since 3.0.0
 */
@State(Scope.Benchmark)
public class CompletableFutureBenchmark {

	@Benchmark
	public void oneCallMethod(Blackhole blackhole) {
		CompletableFuture<Integer> promise = new CompletableFuture<>();
		promise.thenApply(a -> a + a).whenComplete((result, e) -> blackhole.consume(result));
		promise.complete(10);
	}

	@Benchmark
	public void combineMeasure(Blackhole blackhole) {
		CompletableFuture<Integer> promise = new CompletableFuture<>();
		CompletableFuture<Integer> additional = new CompletableFuture<>();
		promise.thenApply(a -> a + a).thenCompose(a -> additional).whenComplete((result, e) -> blackhole.consume(result));
		promise.complete(10);
		additional.complete(10);
	}

	public static void main(String[] args) throws RunnerException {

		Options opt = new OptionsBuilder()
			.include(CompletableFutureBenchmark.class.getSimpleName())
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


