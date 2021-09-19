package io.activej.rpc;

import io.activej.async.callback.Callback;
import io.activej.common.MemSize;
import io.activej.common.initializer.Initializer;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.eventloop.Eventloop;
import io.activej.inject.Key;
import io.activej.inject.annotation.*;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.server.RpcServer;
import io.activej.service.ServiceGraphModule;
import io.activej.service.ServiceGraphModuleSettings;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;

import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.ConfigConverters.ofFrameFormat;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static java.lang.Math.min;

@SuppressWarnings("WeakerAccess")
public class RpcBenchmark extends Launcher {
	private static final int TOTAL_REQUESTS = 10000000;
	private static final int WARMUP_ROUNDS = 3;
	private static final int BENCHMARK_ROUNDS = 10;
	private static final int SERVICE_PORT = 25565;
	private static final int ACTIVE_REQUESTS_MIN = 10000;
	private static final int ACTIVE_REQUESTS_MAX = 10000;

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
				.withEventloopFatalErrorHandler(rethrow());
	}

	@Provides
	@Named("server")
	Eventloop eventloopServer(@Named("client") Eventloop clientEventloop, Config config) {
		return config.get(ofBoolean(), "multithreaded", true) ?
				Eventloop.create()
						.withEventloopFatalErrorHandler(rethrow()) :
				clientEventloop;
	}

	@Provides
	public RpcClient rpcClient(@Named("client") Eventloop eventloop, Config config) {
		return RpcClient.create(eventloop)
				.withStreamProtocol(
						config.get(ofMemSize(), "rpc.defaultPacketSize", MemSize.kilobytes(256)),
						config.get(ofFrameFormat(), "rpc.compression", null))
				.withMessageTypes(Integer.class)
				.withStrategy(server(new InetSocketAddress(config.get(ofInteger(), "rpc.server.port"))));
	}

	@Provides
	@Eager
	public RpcServer rpcServer(@Named("server") Eventloop eventloop, Config config) {
		return RpcServer.create(eventloop)
				.withStreamProtocol(
						config.get(ofMemSize(), "rpc.defaultPacketSize", MemSize.kilobytes(256)),
						config.get(ofFrameFormat(), "rpc.compression", null))
				.withListenPort(config.get(ofInteger(), "rpc.server.port"))
				.withMessageTypes(Integer.class)
				.withHandler(Integer.class, req -> Promise.of(req * 2));

	}

	@ProvidesIntoSet
	Initializer<ServiceGraphModuleSettings> configureServiceGraph() {
		// add logical dependency so that service graph starts client only after it started the server
		return settings -> settings.addDependency(Key.of(RpcClient.class), Key.of(RpcServer.class));
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
	 * Second counter represents amount of completed requests(in other words completed will be incremented when
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
			public void accept(Integer result, @Nullable Exception e) {
				if (e != null) return;
				completed++;

				int active = sent - completed;

				// Stop round
				if (completed == totalRequests) {
					promise.set(null);
					return;
				}

				if (active <= activeRequestsMin) {
					for (int i = 0; i < min(activeRequestsMax - active, totalRequests - sent); i++, sent++) {
						rpcClient.sendRequest(sent, this);
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
		RpcBenchmark benchmark = new RpcBenchmark();
		benchmark.launch(args);
	}
}
