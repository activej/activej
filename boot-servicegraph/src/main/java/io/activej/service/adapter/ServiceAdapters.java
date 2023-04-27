/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.service.adapter;

import io.activej.async.service.ReactiveService;
import io.activej.common.annotation.StaticFactories;
import io.activej.common.service.BlockingService;
import io.activej.eventloop.Eventloop;
import io.activej.inject.binding.OptionalDependency;
import io.activej.net.ReactiveServer;
import io.activej.reactor.net.BlockingSocketServer;
import io.activej.service.Service;
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;

import static org.slf4j.LoggerFactory.getLogger;

@SuppressWarnings("WeakerAccess")
@StaticFactories(ServiceAdapter.class)
public class ServiceAdapters {
	private static final Logger logger = getLogger(ServiceAdapters.class);

	public abstract static class SimpleServiceAdapter<S> implements ServiceAdapter<S> {
		private final boolean startConcurrently;
		private final boolean stopConcurrently;

		protected SimpleServiceAdapter(boolean startConcurrently, boolean stopConcurrently) {
			this.startConcurrently = startConcurrently;
			this.stopConcurrently = stopConcurrently;
		}

		protected SimpleServiceAdapter() {
			this(true, true);
		}

		protected abstract void start(S instance) throws Exception;

		protected abstract void stop(S instance) throws Exception;

		@Override
		public final CompletableFuture<?> start(S instance, Executor executor) {
			CompletableFuture<?> future = new CompletableFuture<>();
			(startConcurrently ? executor : (Executor) Runnable::run).execute(() -> {
				try {
					start(instance);
					future.complete(null);
				} catch (Exception e) {
					future.completeExceptionally(e);
				}
			});
			return future;
		}

		@Override
		public final CompletableFuture<?> stop(S instance, Executor executor) {
			CompletableFuture<?> future = new CompletableFuture<>();
			(stopConcurrently ? executor : (Executor) Runnable::run).execute(() -> {
				try {
					stop(instance);
					future.complete(null);
				} catch (Exception e) {
					future.completeExceptionally(e);
				}
			});
			return future;
		}
	}

	public static ServiceAdapter<Service> forService() {
		return new ServiceAdapter<>() {
			@Override
			public CompletableFuture<?> start(Service instance, Executor executor) {
				return instance.start();
			}

			@Override
			public CompletableFuture<?> stop(Service instance, Executor executor) {
				return instance.stop();
			}
		};
	}

	/**
	 * Returns factory which transforms blocking Service to asynchronous non-blocking CompletableFuture. It runs blocking operations from other thread from
	 * executor.
	 */
	public static ServiceAdapter<BlockingService> forBlockingService() {
		return new SimpleServiceAdapter<>() {
			@Override
			protected void start(BlockingService instance) throws Exception {
				instance.start();
			}

			@Override
			protected void stop(BlockingService instance) throws Exception {
				instance.stop();
			}
		};
	}

	public static <T> ServiceAdapter<OptionalDependency<T>> forOptionalDependency(ServiceAdapter<T> adapter) {
		return new ServiceAdapter<>() {
			@Override
			public CompletableFuture<?> start(OptionalDependency<T> optional, Executor executor) {
				if (optional.isPresent()) {
					T instance = optional.get();
					return adapter.start(instance, executor);
				}
				return CompletableFuture.completedFuture(null);
			}

			@Override
			public CompletableFuture<?> stop(OptionalDependency<T> optional, Executor executor) {
				if (optional.isPresent()) {
					T instance = optional.get();
					return adapter.stop(instance, executor);
				}
				return CompletableFuture.completedFuture(null);
			}
		};
	}

	/**
	 * Returns factory which transforms Timer to CompletableFuture. On start it does nothing, on stop it cancel timer.
	 */
	public static ServiceAdapter<Timer> forTimer() {
		return new SimpleServiceAdapter<>(false, false) {
			@Override
			protected void start(Timer instance) {
			}

			@Override
			protected void stop(Timer instance) {
				instance.cancel();
			}
		};
	}

	/**
	 * Returns factory which transforms ExecutorService to CompletableFuture. On starting it doing nothing, on stopping it shuts down ExecutorService.
	 */
	public static ServiceAdapter<ExecutorService> forExecutorService() {
		return new SimpleServiceAdapter<>(false, true) {
			@Override
			protected void start(ExecutorService instance) {
			}

			@Override
			protected void stop(ExecutorService instance) throws Exception {
				instance.shutdown();
				if (!instance.isTerminated()) {
					logger.warn("Awaiting termination of {} ...", instance);
					//noinspection ResultOfMethodCallIgnored
					instance.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
					logger.info("Instance {} has been terminated", instance);
				}
			}
		};
	}

	/**
	 * Returns factory which transforms AutoCloseable object to CompletableFuture. On starting it doing nothing, on stopping it close AutoCloseable.
	 */
	public static ServiceAdapter<AutoCloseable> forAutoCloseable() {
		return new SimpleServiceAdapter<>(false, true) {
			@Override
			protected void start(AutoCloseable instance) {
			}

			@Override
			protected void stop(AutoCloseable instance) throws Exception {
				instance.close();
			}
		};
	}

	/**
	 * Returns factory which transforms DataSource object to CompletableFuture. On start it checks connecting , on stop it closes DataSource.
	 */
	public static ServiceAdapter<DataSource> forDataSource() {
		return new SimpleServiceAdapter<>(true, false) {
			@Override
			protected void start(DataSource instance) throws Exception {
				Connection connection = instance.getConnection();
				connection.close();
			}

			@Override
			protected void stop(DataSource instance) {
			}
		};
	}

	public static ServiceAdapter<ReactiveService> forReactiveService() {
		return new ServiceAdapter<>() {
			@Override
			public CompletableFuture<?> start(ReactiveService instance, Executor executor) {
				CompletableFuture<Object> future = new CompletableFuture<>();
				instance.getReactor().execute(() -> {
					try {
						instance.start()
								.whenResult(future::complete)
								.whenException(future::completeExceptionally);
					} catch (Exception e) {
						future.completeExceptionally(e);
					}
				});
				return future;
			}

			@Override
			public CompletableFuture<?> stop(ReactiveService instance, Executor executor) {
				CompletableFuture<Object> future = new CompletableFuture<>();
				instance.getReactor().execute(() -> {
					try {
						instance.stop()
								.whenResult(future::complete)
								.whenException(future::completeExceptionally);
					} catch (Exception e) {
						future.completeExceptionally(e);
					}
				});
				return future;
			}
		};
	}

	public static ServiceAdapter<ReactiveServer> forReactiveServer() {
		return new ServiceAdapter<>() {
			@Override
			public CompletableFuture<?> start(ReactiveServer instance, Executor executor) {
				CompletableFuture<?> future = new CompletableFuture<>();
				instance.getReactor().execute(() -> {
					try {
						instance.listen();
						future.complete(null);
					} catch (IOException e) {
						future.completeExceptionally(e);
					}
				});
				return future;
			}

			@Override
			public CompletableFuture<?> stop(ReactiveServer instance, Executor executor) {
				CompletableFuture<Object> future = new CompletableFuture<>();
				instance.getReactor().execute(() -> instance.close()
						.whenResult(future::complete)
						.whenException(future::completeExceptionally));
				return future;
			}
		};
	}

	public static ServiceAdapter<Eventloop> forEventloop(ThreadFactory threadFactory) {
		return new ServiceAdapter<>() {
			@Override
			public CompletableFuture<?> start(Eventloop eventloop, Executor executor) {
				CompletableFuture<?> future = new CompletableFuture<>();
				threadFactory.newThread(() -> {
					eventloop.keepAlive(true);
					future.complete(null);
					eventloop.run();
				}).start();
				return future;
			}

			@Override
			public CompletableFuture<?> stop(Eventloop eventloop, Executor executor) {
				Thread eventloopThread = eventloop.getEventloopThread();
				if (eventloopThread == null) {
					// already stopped
					return CompletableFuture.completedFuture(null);
				}
				CompletableFuture<?> future = new CompletableFuture<>();
				eventloop.execute(() -> {
					eventloop.keepAlive(false);
					logStopping(eventloop);
					Eventloop.logger.info("Waiting for {}", eventloop);
				});
				executor.execute(() -> {
					try {
						eventloopThread.join();
						future.complete(null);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						future.completeExceptionally(e);
					}
				});
				return future;
			}

			private void logStopping(Eventloop eventloop) {
				eventloop.delayBackground(1000L, () -> {
					if (eventloop.getEventloopThread() != null) {
						Eventloop.logger.info("...Waiting for {}", eventloop);
						logStopping(eventloop);
					}
				});
			}
		};
	}

	public static ServiceAdapter<Eventloop> forEventloop() {
		ThreadFactory threadFactory = Executors.defaultThreadFactory();
		return forEventloop(r -> {
			Thread thread = threadFactory.newThread(r);
			thread.setName("eventloop: " + thread.getName());
			return thread;
		});
	}

	public static ServiceAdapter<BlockingSocketServer> forBlockingSocketServer() {
		return new ServiceAdapters.SimpleServiceAdapter<>() {
			@Override
			protected void start(BlockingSocketServer instance) throws Exception {
				instance.start();
			}

			@Override
			protected void stop(BlockingSocketServer instance) throws Exception {
				instance.stop();
			}
		};
	}

	public static <T> ServiceAdapter<T> immediateServiceAdapter() {
		return new SimpleServiceAdapter<>(false, false) {
			@Override
			protected void start(T instance) {
			}

			@Override
			protected void stop(T instance) {
			}
		};
	}

	@SafeVarargs
	public static <T, S extends ServiceAdapter<? super T>> ServiceAdapter<T> combinedAdapter(S... startOrder) {
		return combinedAdapter(List.of(startOrder));
	}

	public static <T> ServiceAdapter<T> combinedAdapter(List<? extends ServiceAdapter<? super T>> startOrder) {
		List<? extends ServiceAdapter<? super T>> stopOrder = new ArrayList<>(startOrder);
		Collections.reverse(stopOrder);
		return combinedAdapter(startOrder, stopOrder);
	}

	@FunctionalInterface
	public interface Action<T> {
		CompletableFuture<?> doAction(ServiceAdapter<T> serviceAdapter, T instance, Executor executor);
	}

	public static <T> ServiceAdapter<T> combinedAdapter(List<? extends ServiceAdapter<? super T>> startOrder,
			List<? extends ServiceAdapter<? super T>> stopOrder) {
		return new ServiceAdapter<>() {
			@SuppressWarnings("unchecked")
			private void doAction(T instance, Executor executor,
					Iterator<? extends ServiceAdapter<? super T>> iterator, CompletableFuture<?> future,
					Action<T> action) {
				if (iterator.hasNext()) {
					action.doAction((ServiceAdapter<T>) iterator.next(), instance, executor)
							.whenCompleteAsync(($, e) -> {
								if (e == null) {
									doAction(instance, executor, iterator, future, action);
								} else if (e instanceof InterruptedException) {
									Thread.currentThread().interrupt();
									future.completeExceptionally(e);
								} else if (e instanceof ExecutionException) {
									future.completeExceptionally(e.getCause());
								}
							}, Runnable::run);
				} else {
					future.complete(null);
				}
			}

			@Override
			public CompletableFuture<Void> start(T instance, Executor executor) {
				CompletableFuture<Void> future = new CompletableFuture<>();
				doAction(instance, executor, startOrder.iterator(), future,
						ServiceAdapter::start);
				return future;
			}

			@Override
			public CompletableFuture<Void> stop(T instance, Executor executor) {
				CompletableFuture<Void> future = new CompletableFuture<>();
				doAction(instance, executor, stopOrder.iterator(), future,
						ServiceAdapter::stop);
				return future;
			}
		};
	}
}
