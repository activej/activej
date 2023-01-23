package io.activej.datastream;

import io.activej.config.Config;
import io.activej.datastream.processor.StreamFilter;
import io.activej.eventloop.Eventloop;
import io.activej.inject.InstanceProvider;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.annotation.Transient;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.service.ServiceGraphModule;

import java.util.function.Function;

import static io.activej.config.converter.ConfigConverters.ofInteger;

@SuppressWarnings("WeakerAccess")
public class DatastreamBenchmark extends Launcher {
	private static final int TOTAL_ELEMENTS = 100_000_000;
	private static final int WARMUP_ROUNDS = 3;
	private static final int BENCHMARK_ROUNDS = 10;

	static final class IntegerStreamSupplier extends AbstractStreamSupplier<Integer> {
		private Integer integer;
		private final int limit;

		public IntegerStreamSupplier(int limit) {
			this.integer = 0;
			this.limit = limit;
		}

		@Override
		protected void onResumed() {
			while (integer < limit) {
				send(++integer);
			}
			sendEndOfStream();
		}
	}

	@Inject
	Reactor reactor;

	@Inject
	Config config;

	@Inject
	InstanceProvider<StreamSupplier<Integer>> inputProvider;

	@Inject
	InstanceProvider<StreamFilter<Integer, Integer>> mapperProvider;

	@Inject
	InstanceProvider<StreamConsumer<Integer>> outputProvider;

	@Provides
	Reactor reactor() {
		return Eventloop.builder()
				.withCurrentThread()
				.build();
	}

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@Provides
	@Transient
	StreamSupplier<Integer> streamSupplier(Config config) {
		int limit = config.get(ofInteger(), "benchmark.totalElements", TOTAL_ELEMENTS);
		return new IntegerStreamSupplier(limit);
	}

	@Provides
	@Transient
	StreamFilter<Integer, Integer> mapper() {
		return StreamFilter.mapper(Function.identity());
	}

	@Provides
	@Transient
	StreamConsumer<Integer> streamConsumer() {
		return StreamConsumer.skip();
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	private int warmupRounds;
	private int benchmarkRounds;
	private int totalElements;

	@Override
	protected void onStart() {
		warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds", WARMUP_ROUNDS);
		benchmarkRounds = config.get(ofInteger(), "benchmark.benchmarkRounds", BENCHMARK_ROUNDS);
		totalElements = config.get(ofInteger(), "benchmark.totalElements", TOTAL_ELEMENTS);
	}

	@Override
	protected void run() throws Exception {
		long time = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Warming up ...");
		for (int i = 0; i < warmupRounds; i++) {
			long roundTime = round();
			long rps = totalElements * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms; OPS : " + rps);
		}

		System.out.println("Start benchmarking...");

		for (int i = 0; i < benchmarkRounds; i++) {
			long roundTime = round();

			time += roundTime;

			if (bestTime == -1 || roundTime < bestTime) {
				bestTime = roundTime;
			}

			if (worstTime == -1 || roundTime > worstTime) {
				worstTime = roundTime;
			}

			long rps = totalElements * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms; OPS : " + rps);
		}
		double avgTime = (double) time / benchmarkRounds;
		long requestsPerSecond = (long) (totalElements / avgTime * 1000);
		System.out.println("Time: " + time + "ms; Average time: " + avgTime + "ms; Best time: " +
				bestTime + "ms; Worst time: " + worstTime + "ms; Operations per second: " + requestsPerSecond);
	}

	private long round() throws Exception {
		return reactor.submit(this::roundCall).get();
	}

	private Promise<Long> roundCall() {
		StreamSupplier<Integer> input = inputProvider.get();
		StreamFilter<Integer, Integer> mapper = mapperProvider.get();
		StreamConsumer<Integer> output = outputProvider.get();
		long start = System.currentTimeMillis();
		return input
				.transformWith(mapper)
				.streamTo(output)
				.map($ -> System.currentTimeMillis() - start);
	}

	public static void main(String[] args) throws Exception {
		DatastreamBenchmark benchmark = new DatastreamBenchmark();
		benchmark.launch(args);
	}
}
