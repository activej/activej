package io.activej.cube.service;

import io.activej.etcd.state.AbstractEtcdStateManager;
import io.activej.ot.OTStateManager;
import io.activej.promise.Promise;

import java.util.List;

public interface ServiceStateManager<D> {
	Promise<Void> checkout();

	Promise<Void> sync();

	Promise<Void> push(List<D> diffs);

	void reset();

	static <D> ServiceStateManager<D> ofOTStateManager(OTStateManager<?, D> stateManager) {
		return new ServiceStateManager<>() {
			@Override
			public Promise<Void> checkout() {
				return stateManager.checkout();
			}

			@Override
			public Promise<Void> sync() {
				return stateManager.sync();
			}

			@Override
			public Promise<Void> push(List<D> diffs) {
				stateManager.addAll(diffs);
				return sync();
			}

			@Override
			public void reset() {
				stateManager.reset();
			}
		};
	}

	static <D, S> ServiceStateManager<D> ofEtcdStateManager(AbstractEtcdStateManager<S, List<D>> stateManager) {
		return new ServiceStateManager<>() {
			@Override
			public Promise<Void> checkout() {
				return Promise.complete();
			}

			@Override
			public Promise<Void> sync() {
				return Promise.complete();
			}

			@Override
			public Promise<Void> push(List<D> diffs) {
				return Promise.ofFuture(stateManager.push(diffs)).toVoid();
			}

			@Override
			public void reset() {
			}
		};
	}
}
