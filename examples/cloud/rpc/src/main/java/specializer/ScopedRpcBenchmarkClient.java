package specializer;

import io.activej.async.callback.Callback;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.rpc.client.RpcClient;
import io.activej.service.ServiceGraphModule;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;

import static io.activej.common.exception.FatalErrorHandler.rethrow;
import static io.activej.rpc.client.sender.RpcStrategies.server;
import static java.lang.Math.min;
import static specializer.ScopedRpcServerExample.PORT;

/**
 * Run this benchmark client after launching {@link ScopedRpcServerExample}
 */
public final class ScopedRpcBenchmarkClient extends Launcher {
	private static final int TOTAL_REQUESTS = 5_000_000;
	private static final int WARMUP_ROUNDS = 3;
	private static final int BENCHMARK_ROUNDS = 10;
	private static final int ACTIVE_REQUESTS_MIN = 10_000;
	private static final int ACTIVE_REQUESTS_MAX = 10_000;

	@Inject
	RpcClient client;

	@Inject
	Eventloop eventloop;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create()
				.withEventloopFatalErrorHandler(rethrow());
	}

	@Provides
	public RpcClient client(Eventloop eventloop) {
		return RpcClient.create(eventloop)
				.withMessageTypes(RpcRequest.class, RpcResponse.class)
				.withStrategy(server(new InetSocketAddress(PORT)));
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	int sent;
	int completed;

	@Override
	protected void run() throws Exception {
		long time = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Warming up ...");
		for (int i = 0; i < WARMUP_ROUNDS; i++) {
			long roundTime = round();
			long rps = TOTAL_REQUESTS * 1000L / roundTime;
			System.out.printf("Round: %d; Round time: %dms; RPS : %d%n", i + 1, roundTime, rps);
		}

		System.out.println("Start benchmarking RPC");

		for (int i = 0; i < BENCHMARK_ROUNDS; i++) {
			long roundTime = round();

			time += roundTime;

			if (bestTime == -1 || roundTime < bestTime) {
				bestTime = roundTime;
			}

			if (worstTime == -1 || roundTime > worstTime) {
				worstTime = roundTime;
			}

			long rps = TOTAL_REQUESTS * 1000L / roundTime;
			System.out.printf("Round: %d; Round time: %dms; RPS : %d%n", i + 1, roundTime, rps);
		}
		double avgTime = (double) time / BENCHMARK_ROUNDS;
		long requestsPerSecond = (long) (TOTAL_REQUESTS / avgTime * 1000);
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

		Callback<RpcResponse> callback = new Callback<>() {
			@Override
			public void accept(RpcResponse result, @Nullable Exception e) {
				completed++;

				int active = sent - completed;

				// Stop round
				if (completed == TOTAL_REQUESTS) {
					promise.set(null);
					return;
				}

				if (active <= ACTIVE_REQUESTS_MIN) {
					for (int i = 0; i < min(ACTIVE_REQUESTS_MAX - active, TOTAL_REQUESTS - sent); i++, sent++) {
						client.sendRequest(new RpcRequest(sent), this);
					}
				}
			}
		};

		for (int i = 0; i < min(ACTIVE_REQUESTS_MAX, TOTAL_REQUESTS); i++) {
			client.sendRequest(new RpcRequest(sent), callback);
			sent++;
		}

		return promise.map($ -> System.currentTimeMillis() - start);
	}

	public static void main(String[] args) throws Exception {
		ScopedRpcBenchmarkClient benchmark = new ScopedRpcBenchmarkClient();
		benchmark.launch(args);
	}
}
