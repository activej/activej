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

package io.activej.test;

import io.activej.common.function.*;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public final class TestUtils {
	private static int activePromises = 0;

	static int port = 1024;

	public static synchronized int getFreePort() {
		while (++port < 65536) {
			if (!probeBindAddress(new InetSocketAddress(port))) continue;
			if (!probeBindAddress(new InetSocketAddress("localhost", port))) continue;
			if (!probeBindAddress(new InetSocketAddress("127.0.0.1", port))) continue;
			return port;
		}
		throw new AssertionError();
	}

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	private static boolean probeBindAddress(InetSocketAddress inetSocketAddress) {
		try (ServerSocket s = new ServerSocket()) {
			s.bind(inetSocketAddress);
		} catch (BindException e) {
			return false;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return true;
	}

	public static <T> BiConsumerEx<T, Exception> assertCompleteFn(ConsumerEx<T> consumer) {
		activePromises++;
		return (t, e) -> {
			activePromises--;
			if (e != null) {
//				if (e instanceof AssertionError) {
//					throw (AssertionError) e;
//				}
				throw new AssertionError(e);
			}
			try {
				consumer.accept(t);
			} catch (AssertionError e2) {
				throw e2;
			} catch (Throwable e2) {
				throw new AssertionError(e2);
			}
		};
	}

	public static <T> BiConsumerEx<T, Exception> assertCompleteFn() {
		return assertCompleteFn($ -> {});
	}

	public static int getActivePromises() {
		return activePromises;
	}

	public static void clearActivePromises() {
		activePromises = 0;
	}

	public static <T> Supplier<T> assertingFn(SupplierEx<T> fn) {
		return () -> {
			try {
				return fn.get();
			} catch (RuntimeException | Error e) {
				throw e;
			} catch (Throwable e) {
				throw new AssertionError(e);
			}
		};
	}

	public static <T> Consumer<T> assertingFn(ConsumerEx<T> fn) {
		return x -> {
			try {
				fn.accept(x);
			} catch (RuntimeException | Error e) {
				throw e;
			} catch (Throwable e) {
				throw new AssertionError(e);
			}
		};
	}

	public static <T, U> BiConsumerEx<T, U> assertingFn(BiConsumerEx<T, U> fn) {
		return (x, y) -> {
			try {
				fn.accept(x, y);
			} catch (RuntimeException | Error e) {
				throw e;
			} catch (Throwable throwable) {
				throw new AssertionError(throwable);
			}
		};
	}

	public static <T, R> Function<T, R> assertingFn(FunctionEx<T, R> fn) {
		return x -> {
			try {
				return fn.apply(x);
			} catch (RuntimeException | Error e) {
				throw e;
			} catch (Throwable e) {
				throw new AssertionError(e);
			}
		};
	}

	public static <T, U, R> BiFunction<T, U, R> assertingFn(BiFunctionEx<T, U, R> fn) {
		return (x, y) -> {
			try {
				return fn.apply(x, y);
			} catch (RuntimeException | Error e) {
				throw e;
			} catch (Throwable e) {
				throw new AssertionError(e);
			}
		};
	}
}
