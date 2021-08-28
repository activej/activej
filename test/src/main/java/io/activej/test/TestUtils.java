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

import com.mysql.cj.jdbc.MysqlDataSource;
import io.activej.async.callback.Callback;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.io.*;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.*;

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

	public static DataSource dataSource(String databasePropertiesPath) throws IOException, SQLException {
		Properties properties = new Properties();
		properties.load(new InputStreamReader(new BufferedInputStream(new FileInputStream(new File(databasePropertiesPath))), StandardCharsets.UTF_8));

		MysqlDataSource dataSource = new MysqlDataSource();
		dataSource.setUrl("jdbc:mysql://" + properties.getProperty("dataSource.serverName") + '/' + properties.getProperty("dataSource.databaseName"));
		dataSource.setUser(properties.getProperty("dataSource.user"));
		dataSource.setPassword(properties.getProperty("dataSource.password"));
		dataSource.setServerTimezone(properties.getProperty("dataSource.timeZone"));
		dataSource.setAllowMultiQueries(true);
		return dataSource;
	}

	public static <T> BiConsumer<T, Throwable> assertComplete(ThrowingConsumer<T> consumer) {
		activePromises++;
		return (t, e) -> {
			activePromises--;
			if (e != null) {
				if (e instanceof AssertionError) {
					throw (AssertionError) e;
				}
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

	public static <T> BiConsumer<T, Throwable> assertComplete() {
		return assertComplete($ -> {});
	}

	public static int getActivePromises() {
		return activePromises;
	}

	public static void clearActivePromises() {
		activePromises = 0;
	}

	@FunctionalInterface
	public interface ThrowingSupplier<T> {
		T get() throws Throwable;
	}

	public static <T> Supplier<T> asserting(ThrowingSupplier<T> supplier) {
		return () -> {
			try {
				return supplier.get();
			} catch (AssertionError e) {
				throw e;
			} catch (Throwable e) {
				throw new AssertionError(e);
			}
		};
	}

	@FunctionalInterface
	public interface ThrowingConsumer<T> {
		void accept(T t) throws Throwable;
	}

	public static <T> Consumer<T> asserting(ThrowingConsumer<T> consumer) {
		return x -> {
			try {
				consumer.accept(x);
			} catch (AssertionError e) {
				throw e;
			} catch (Throwable e) {
				throw new AssertionError(e);
			}
		};
	}

	@FunctionalInterface
	public interface ThrowingConsumerEx<T> {
		void accept(T result, @Nullable Throwable e) throws Throwable;
	}

	public static <T> BiConsumer<T, Throwable> asserting(ThrowingConsumerEx<T> consumer) {
		return (x, y) -> {
			try {
				consumer.accept(x, y);
			} catch (RuntimeException | Error e) {
				throw e;
			} catch (Throwable throwable) {
				throw new AssertionError(throwable);
			}
		};
	}

	@FunctionalInterface
	public interface ThrowingFunction<T, R> {
		R apply(T t) throws Throwable;
	}

	public static <T, R> Function<T, R> asserting(ThrowingFunction<T, R> function) {
		return x -> {
			try {
				return function.apply(x);
			} catch (AssertionError e) {
				throw e;
			} catch (Throwable e) {
				throw new AssertionError(e);
			}
		};
	}

	@FunctionalInterface
	public interface ThrowingBiFunction<T, U, R> {
		R apply(T t, U u) throws Throwable;
	}

	public static <T, U, R> BiFunction<T, U, R> asserting(ThrowingBiFunction<T, U, R> function) {
		return (x, y) -> {
			try {
				return function.apply(x, y);
			} catch (AssertionError e) {
				throw e;
			} catch (Throwable e) {
				throw new AssertionError(e);
			}
		};
	}
}
