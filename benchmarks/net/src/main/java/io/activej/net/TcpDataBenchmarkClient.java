package io.activej.net;

import io.activej.config.Config;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.datastream.AbstractStreamSupplier;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;

import java.net.InetSocketAddress;

import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.config.converter.ConfigConverters.ofInteger;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;

@SuppressWarnings("WeakerAccess")
public class TcpDataBenchmarkClient extends Launcher {
	private static final int TOTAL_ELEMENTS = 100_000_000;
	private static final int WARMUP_ROUNDS = 3;
	private static final int BENCHMARK_ROUNDS = 10;

	private int items;
	private int warmupRounds;
	private int benchmarkRounds;

	@Inject
	@Named("benchmark")
	NioReactor benchmarkReactor;

	@Inject
	@Named("client")
	NioReactor clientReactor;

	@Inject
	Config config;

	@Provides
	@Named("benchmark")
	NioReactor benchmarkReactor() { return Eventloop.create(); }

	@Provides
	@Named("client")
	NioReactor clientReactor() { return Eventloop.create(); }

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void onStart() {
		this.items = config.get(ofInteger(), "benchmark.totalElements", TOTAL_ELEMENTS);
		this.warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds", WARMUP_ROUNDS);
		this.benchmarkRounds = config.get(ofInteger(), "benchmark.benchmarkRounds", BENCHMARK_ROUNDS);
	}

	@Override
	protected void run() throws Exception {
		long timeAllRounds = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Warming up ...");
		for (int i = 0; i < warmupRounds; i++) {
			long roundTime = round();
			long rps = roundTime != 0 ? (items * 1000L / roundTime) : 0;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms; RPS : " + rps);
		}

		System.out.println("Start benchmarking TCP Echo Server");
		for (int i = 0; i < benchmarkRounds; i++) {
			long roundTime = round();
			timeAllRounds += roundTime;

			if (bestTime == -1 || roundTime < bestTime) {
				bestTime = roundTime;
			}

			if (worstTime == -1 || roundTime > worstTime) {
				worstTime = roundTime;
			}

			long rps = items * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms; RPS : " + rps);
		}

		long avgRoundTime = timeAllRounds / benchmarkRounds;
		long avgRps = avgRoundTime != 0 ? (items * benchmarkRounds * 1000L / timeAllRounds) : 0;
		System.out.println("Total time: " + timeAllRounds + "ms; Average round time: " + avgRoundTime + "ms; Best time: " +
				bestTime + "ms; Worst time: " + worstTime + "ms; Average RPS: " + avgRps);
	}

	private long round() throws Exception {
		return benchmarkReactor.submit(this::roundGet).get();
	}

	private Promise<Long> roundGet() {
		long start = System.currentTimeMillis();

		InetSocketAddress address = config.get(ofInetSocketAddress(), "echo.address", new InetSocketAddress(9001));
		int limit = config.get(ofInteger(), "benchmark.totalElements", TOTAL_ELEMENTS);

		return TcpSocket.connect(clientReactor, address)
				.then(socket -> {
					StreamSupplierOfSequence.create(limit)
							.transformWith(ChannelSerializer.create(INT_SERIALIZER))
							.streamTo(ChannelConsumer.ofSocket(socket));

					return ChannelSupplier.ofSocket(socket)
							.transformWith(ChannelDeserializer.create(INT_SERIALIZER))
							.streamTo(StreamConsumer.skip())
							.whenComplete(socket::close)
							.map($ -> System.currentTimeMillis() - start);
				});
	}

	static final class StreamSupplierOfSequence extends AbstractStreamSupplier<Integer> {
		private int value;
		private final int limit;

		private StreamSupplierOfSequence(int limit) {
			this.value = 0;
			this.limit = limit;
		}

		public static StreamSupplierOfSequence create(int limit) {
			return new StreamSupplierOfSequence(limit);
		}

		@Override
		protected void onResumed() {
			while (value < limit) {
				StreamDataAcceptor<Integer> dataAcceptor = getDataAcceptor();
				if (dataAcceptor == null) {
					return;
				}
				dataAcceptor.accept(++value);
			}
			sendEndOfStream();
		}
	}

	public static void main(String[] args) throws Exception {
		Launcher benchmark = new TcpDataBenchmarkClient();
		benchmark.launch(args);
	}

}
