package io.activej.service;

import io.activej.di.Injector;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class TestStartTwice {
	private static final AtomicInteger countStart = new AtomicInteger(0);
	private static final AtomicInteger countStop = new AtomicInteger(0);

	interface A extends Service {}

	static class ServiceImpl implements A {
		@Override
		public CompletableFuture<Void> start() {
			countStart.incrementAndGet();
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public CompletableFuture<Void> stop() {
			countStop.incrementAndGet();
			return CompletableFuture.completedFuture(null);
		}
	}

	static class TestModule extends AbstractModule {

		@Override
		protected void configure() {
			install(ServiceGraphModule.create());
		}

		@Provides
		ServiceImpl serviceImpl(A a) {
			return (ServiceImpl) a;
		}

		@Provides
		A a() {
			return new ServiceImpl();
		}

	}

	@Test
	public void test() throws Exception {
		Injector injector = Injector.of(new TestModule());
		injector.getInstance(ServiceImpl.class);
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);

		try {
			serviceGraph.startFuture().get();
		} finally {
			serviceGraph.stopFuture().get();
		}

		assertEquals(1, countStart.get());
		assertEquals(1, countStop.get());
	}
}
