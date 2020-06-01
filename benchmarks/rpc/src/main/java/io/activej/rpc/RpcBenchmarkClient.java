package io.activej.rpc;

import io.activej.async.callback.Callback;
import io.activej.common.MemSize;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.di.annotation.Inject;
import io.activej.di.annotation.Named;
import io.activej.di.annotation.Provides;
import io.activej.di.module.Module;
import io.activej.eventloop.Eventloop;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.rpc.client.RpcClient;
import io.activej.service.ServiceGraphModule;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;

import static io.activej.config.ConfigConverters.*;
import static io.activej.di.module.Modules.combine;
import static io.activej.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static java.lang.Math.min;

@SuppressWarnings("WeakerAccess")
public class RpcBenchmarkClient extends Launcher {
	private final static int TOTAL_REQUESTS = 10000000;
	private final static int WARMUP_ROUNDS = 3;
	private final static int BENCHMARK_ROUNDS = 10;
	private final static int SERVICE_PORT = 25565;
	private final static int ACTIVE_REQUESTS_MIN = 10000;
	private final static int ACTIVE_REQUESTS_MAX = 10000;

	@Inject
	RpcClient rpcClient;

	@Inject
	@Named("client")
	Eventloop eventloop;

	@Inject
	Config config;

	@Provides
	@Named("client")
	Eventloop eventloopClient() {
		return Eventloop.create()
				.withFatalErrorHandler(rethrowOnAnyError());
	}

	@Provides
	public RpcClient rpcClient(@Named("client") Eventloop eventloop, Config config) {
		return RpcClient.create(eventloop)
				.withStreamProtocol(
						config.get(ofMemSize(), "rpc.defaultPacketSize", MemSize.kilobytes(256)),
						MemSize.bytes(128),
						config.get(ofBoolean(), "rpc.compression", false))
				.withMessageTypes(Integer.class)
				.withStrategy(server(new InetSocketAddress(config.get(ofInteger(), "rpc.server.port"))));
	}

	@Provides
	Config config() {
		return Config.create()
				.with("rpc.server.port", "" + SERVICE_PORT)
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger());
	}

	private int warmupRounds;
	private int benchmarkRounds;
	private int totalRequests;
	private int activeRequestsMin;
	private int activeRequestsMax;

	@Override
	protected void onStart() {
		warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds", WARMUP_ROUNDS);
		benchmarkRounds = config.get(ofInteger(), "benchmark.benchmarkRounds", BENCHMARK_ROUNDS);
		totalRequests = config.get(ofInteger(), "benchmark.totalRequests", TOTAL_REQUESTS);
		activeRequestsMin = config.get(ofInteger(), "benchmark.activeRequestsMin", ACTIVE_REQUESTS_MIN);
		activeRequestsMax = config.get(ofInteger(), "benchmark.activeRequestsMax", ACTIVE_REQUESTS_MAX);
	}

	/**
	 * First counter represents amount of sent requests, so we know when to stop sending them
	 * Second counter represents amount of completed requests(in another words completed will be incremented when
	 * request fails or completes successfully) so we know when to stop round of benchmark
	 */
	int sent;
	int completed;

	@Override
	protected void run() throws Exception {
		long time = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Warming up ...");
		for (int i = 0; i < warmupRounds; i++) {
			long roundTime = round();
			long rps = totalRequests * 1000L / roundTime;
			System.out.printf("Round: %d; Round time: %dms; RPS : %d%n", i + 1, roundTime, rps);
		}

		System.out.println("Start benchmarking RPC");

		for (int i = 0; i < benchmarkRounds; i++) {
			long roundTime = round();

			time += roundTime;

			if (bestTime == -1 || roundTime < bestTime) {
				bestTime = roundTime;
			}

			if (worstTime == -1 || roundTime > worstTime) {
				worstTime = roundTime;
			}

			long rps = totalRequests * 1000L / roundTime;
			System.out.printf("Round: %d; Round time: %dms; RPS : %d%n", i + 1, roundTime, rps);
		}
		double avgTime = (double) time / benchmarkRounds;
		long requestsPerSecond = (long) (totalRequests / avgTime * 1000);
		System.out.printf("Time: %dms; Average time: %sms; Best time: %dms; Worst time: %dms; Requests per second: %d%n",
				time, avgTime, bestTime, worstTime, requestsPerSecond);
	}

	private long round() throws Exception {
		return eventloop.submit(this::roundCall).get();
	}

	private Promise<Long> roundCall() {
		SettablePromise<Long> promise = new SettablePromise<>();

		long start = System.currentTimeMillis();

		sent = 0;
		completed = 0;

		Callback<Integer> callback = new Callback<Integer>() {
			@Override
			public void accept(Integer result, @Nullable Throwable e) {
				completed++;

				int active = sent - completed;

				// Stop round
				if (completed == totalRequests) {
					promise.set(null);
					return;
				}

				if (active <= activeRequestsMin) {
					for (int i = 0; i < min(activeRequestsMax - active, totalRequests - sent); i++) {
						rpcClient.sendRequest(sent, this);
						sent++;
					}
				}
			}
		};

		for (int i = 0; i < min(activeRequestsMax, totalRequests); i++) {
			rpcClient.sendRequest(sent, callback);
			sent++;
		}

		return promise.map($ -> System.currentTimeMillis() - start);
	}

	public static void main(String[] args) throws Exception {
		RpcBenchmarkClient benchmark = new RpcBenchmarkClient();
		benchmark.launch(args);
	}
}
