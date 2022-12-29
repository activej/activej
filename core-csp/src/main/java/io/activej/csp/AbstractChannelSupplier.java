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

package io.activej.csp;

import io.activej.async.process.AbstractReactiveCloseable;
import io.activej.async.process.ReactiveCloseable;
import io.activej.common.Checks;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.Checks.checkState;

public abstract class AbstractChannelSupplier<T> extends AbstractReactiveCloseable implements ChannelSupplier<T> {
	protected static final boolean CHECK = Checks.isEnabled(AbstractChannelSupplier.class);

	// region creators
	protected AbstractChannelSupplier() {
		setCloseable(null);
	}

	protected AbstractChannelSupplier(@Nullable ReactiveCloseable closeable) {
		setCloseable(closeable);
	}
	// endregion

	protected abstract Promise<T> doGet();

	@Override
	public final Promise<T> get() {
		if (CHECK) checkState(inReactorThread(), "Not in eventloop thread");
		return isClosed() ? Promise.ofException(getException()) : doGet();
	}
}
