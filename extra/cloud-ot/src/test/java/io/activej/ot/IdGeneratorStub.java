package io.activej.ot;

import io.activej.ot.util.IdGenerator;
import io.activej.promise.Promise;

public class IdGeneratorStub implements IdGenerator<Long> {
	public long id;

	public void set(long id) {
		this.id = id;
	}

	@Override
	public Promise<Long> createId() {
		return Promise.of(++id);
	}
}
