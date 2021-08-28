package io.activej.http;

import io.activej.async.callback.Callback;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.service.ServiceGraphModule;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.inject.module.Modules.combine;
import static java.lang.Math.min;

public class HttpServerWorkloadBenchmark extends Launcher {
	private static final int KEEP_ALIVE = 30;
	private static final int TOTAL_REQUESTS = 1_000_000;
	private static final int WARMUP_ROUNDS = 3;
	private static final int BENCHMARK_ROUNDS = 5;
	private static final int ACTIVE_REQUESTS_MAX = 300;
	private static final int ACTIVE_REQUESTS_MIN = 200;

	private String address;
	private int totalRequests;
	private int warmupRounds;
	private int measureRounds;
	private int activeRequestsMax;
	private int activeRequestsMin;

	@Provides
	@Named("server")
	Eventloop serverEventloop() { return Eventloop.create(); }

	@Provides
	@Named("client")
	Eventloop clientEventloop() { return Eventloop.create(); }

	@Inject
	@Named("server")
	Eventloop serverEventloop;

	@Inject
	@Named("client")
	Eventloop clientEventloop;

	@Inject
	Config config;

	@Inject
	AsyncHttpServer server;

	@Inject
	AsyncHttpClient client;

	@Provides
	AsyncHttpServer server() {
		return AsyncHttpServer.create(serverEventloop,
				request ->
						HttpResponse.ok200().withPlainText("Response!!"))
				.withListenAddresses(config.get(ofList(ofInetSocketAddress()), "address"));
	}

	@Provides
	AsyncHttpClient client() {
		return AsyncHttpClient.create(clientEventloop)
				.withKeepAliveTimeout(Duration.ofSeconds(config.get(ofInteger(),
						"client.keepAlive", KEEP_ALIVE)));
	}

	@Provides
	Config config() {
		return Config.create()
				.with("address", "0.0.0.0:9001")
				.with("client.address", "http://127.0.0.1:9001/")
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger());
	}

	@Override
	protected void onStart() {
		this.address = config.get("client.address");
		this.totalRequests = config.get(ofInteger(), "benchmark.totalRequests", TOTAL_REQUESTS);
		this.warmupRounds = config.get(ofInteger(), "benchmark.warmupRounds", WARMUP_ROUNDS);
		this.measureRounds = config.get(ofInteger(), "benchmark.measureRounds", BENCHMARK_ROUNDS);
		this.activeRequestsMax = config.get(ofInteger(), "benchmark.activeRequestsMax", ACTIVE_REQUESTS_MAX);
		this.activeRequestsMin = config.get(ofInteger(), "benchmark.activeRequestsMin", ACTIVE_REQUESTS_MIN);
	}

	@Override
	protected void run() throws Exception {
		long timeAllRounds = 0;
		long bestTime = -1;
		long worstTime = -1;

		System.out.println("Warming up ...");
		for (int i = 0; i < warmupRounds; i++) {
			long roundTime = round();
			long rps = totalRequests * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + "ms; RPS : " + rps);
		}

		System.out.println("Start benchmarking GET Request");
		for (int i = 0; i < measureRounds; i++) {
			long roundTime = round();
			timeAllRounds += roundTime;

			if (bestTime == -1 || roundTime < bestTime) {
				bestTime = roundTime;
			}

			if (worstTime == -1 || roundTime > worstTime) {
				worstTime = roundTime;
			}

			long rps = totalRequests * 1000L / roundTime;
			System.out.println("Round: " + (i + 1) + "; Round time: " + roundTime + " ms; RPS : " + rps);
		}

		double avgTime = (double) timeAllRounds / measureRounds;
		long requestsPerSecond = (long) (totalRequests / avgTime * 1000);
		System.out.println("Time: " + timeAllRounds + "ms; Average time: " + avgTime + " ms; Best time: " +
				bestTime + "ms; Worst time: " + worstTime + "ms; Requests per second: " + requestsPerSecond);
	}

	private long round() throws Exception {
		return clientEventloop.submit(this::roundGet).get();
	}

	int sent;
	int completed;

	private Promise<Long> roundGet() {
		SettablePromise<Long> promise = new SettablePromise<>();

		Callback<HttpResponse> callback = new Callback<HttpResponse>() {
			@Override
			public void accept(HttpResponse result, @Nullable Exception e) {
				completed++;
				int active = sent - completed;

				if (e != null) {
					promise.setException(new FailedRequestException());
					return;
				}

				if (completed == totalRequests) {
					promise.set(null);
					return;
				}

				if (active <= activeRequestsMin) {
					for (int i = 0; i < min(activeRequestsMax - active, totalRequests - sent); i++, sent++) {
						doGet(this);
					}
				}
			}
		};
		sent = 0;
		completed = 0;
		long start = System.currentTimeMillis();

		for (int i = 0; i < min(activeRequestsMin, totalRequests); i++) {
			doGet(callback);
			sent++;
		}

		return promise.map($ -> System.currentTimeMillis() - start);
	}

	private void doGet(Callback<HttpResponse> callback) {
		client.request(HttpRequest.get(address)).run(callback);
	}

	public static void main(String[] args) throws Exception {
		Launcher benchmark = new HttpServerWorkloadBenchmark();
		benchmark.launch(args);
	}

	private static class FailedRequestException extends Exception {}
}
