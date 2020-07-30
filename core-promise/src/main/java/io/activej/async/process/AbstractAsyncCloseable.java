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

package io.activej.async.process;

import io.activej.common.Checks;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.Checks.checkState;
import static io.activej.common.api.Recyclable.tryRecycle;

public abstract class AbstractAsyncCloseable implements AsyncCloseable {
	private static final boolean CHECK = Checks.isEnabled(AbstractAsyncCloseable.class);

	protected final Eventloop eventloop = Eventloop.getCurrentEventloop();

	@Nullable
	private AsyncCloseable closeable;

	private Throwable exception;

	public Throwable getException() {
		return exception;
	}

	public final void setCloseable(@Nullable AsyncCloseable closeable) {
		this.closeable = closeable;
	}

	protected void onClosed(@NotNull Throwable e) {
	}

	protected void onCleanup() {
	}

	@Override
	public final void closeEx(@NotNull Throwable e) {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		if (isClosed()) return;
		exception = e;
		eventloop.post(this::onCleanup);
		onClosed(e);
		if (closeable != null) {
			closeable.closeEx(e);
		}
	}

	public final boolean isClosed() {
		return exception != null;
	}

	@NotNull
	public final <T> Promise<T> sanitize(Promise<T> promise) {
		return promise.async()
				.thenEx(this::sanitize);
	}

	@NotNull
	public final <T> Promise<T> sanitize(T value, @Nullable Throwable e) {
		if (exception != null) {
			tryRecycle(value);
			if (value instanceof AsyncCloseable) {
				((AsyncCloseable) value).closeEx(exception);
			}
			return Promise.ofException(exception);
		}
		if (e == null) {
			return Promise.of(value);
		} else {
			closeEx(e);
			return Promise.ofException(e);
		}
	}

}
