package io.activej.cube;

import io.activej.ot.util.IdGenerator;
import io.activej.promise.Promise;

public class IdGeneratorStub implements IdGenerator<Long> {
	public long id;

	@Override
	public Promise<Long> createId() {
		return Promise.of(++id);
	}
}
