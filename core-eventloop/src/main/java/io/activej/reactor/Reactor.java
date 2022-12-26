package io.activej.reactor;

import io.activej.async.executor.ReactorExecutor;
import io.activej.reactor.schedule.ReactorScheduler;
import org.jetbrains.annotations.Async;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

public interface Reactor extends ReactorExecutor, ReactorScheduler {

	/**
	 * Returns a {@link Reactor} associated with the current thread
	 * (e.g. the {@link Reactor} registered to an inner ThreadLocal).
	 *
	 * @return an {@link Reactor} associated with the current thread
	 * @throws IllegalStateException when there are no Reactor associated with the current thread
	 */
	static @NotNull <R extends Reactor> R getCurrentReactor() {
		//noinspection unchecked
		return (R) ThreadLocalReactor.getCurrentReactor();
	}

	/**
	 * Returns an {@link Reactor} associated with the current thread
	 * (e.g. the {@link Reactor} registered to an inner ThreadLocal)
	 * or {@code null} if no {@link Reactor} is associated with the current thread.
	 *
	 * @return an {@link Reactor} associated with the current thread
	 * or {@code null} if no {@link Reactor} is associated with the current thread
	 * @see #getCurrentReactor()
	 */
	static @Nullable <R extends Reactor> R getCurrentReactorOrNull() {
		//noinspection unchecked
		return (R) ThreadLocalReactor.getCurrentReactorOrNull();
	}

	static void setCurrentReactor(@NotNull Reactor reactor) {
		ThreadLocalReactor.setCurrentReactor(reactor);
	}

	/**
	 * Initializes a piece of code in a context of another {@link Reactor}.
	 * <p>
	 * This method is useful for when you need to initialize a piece of code with another {@link Reactor} context
	 * (a {@link Reactor} that runs in some other thread).
	 *
	 * @param anotherReactor a {@link Reactor} in context of which a piece of code should be initialized
	 * @param runnable       a piece of code to be initialized in a context of another {@link Reactor}
	 */
	static void initWithReactor(@NotNull Reactor anotherReactor, @NotNull Runnable runnable) {
		ThreadLocalReactor.initWithReactor(anotherReactor, runnable);
	}

	/**
	 * Initializes a component in a context of another {@link Reactor}.
	 * <p>
	 * This method is useful for when you need to initialize some component with another {@link Reactor} context
	 * (a {@link Reactor} that runs in some other thread).
	 *
	 * @param anotherReactor a {@link Reactor} in context of which a piece of code should be initialized
	 * @param callable       a supplier of a component to be initialized in a context of another {@link Reactor}
	 */
	static <T> T initWithReactor(@NotNull Reactor anotherReactor, @NotNull Supplier<T> callable) {
		return ThreadLocalReactor.initWithReactor(anotherReactor, callable);
	}

	boolean inReactorThread();

	void post(@NotNull @Async.Schedule Runnable runnable);

	void postLast(@NotNull @Async.Schedule Runnable runnable);

	void postNext(@NotNull @Async.Schedule Runnable runnable);

	void startExternalTask();

	void completeExternalTask();

	void logFatalError(@NotNull Throwable e, @Nullable Object context);
}
