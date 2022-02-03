package adder;

import io.activej.crdt.primitives.CrdtMergable;

public record SimpleSumsCrdtState(float localSum, float otherSum) implements CrdtMergable<SimpleSumsCrdtState> {

	public static SimpleSumsCrdtState of(float localSum) {
		return new SimpleSumsCrdtState(localSum, 0);
	}

	public static SimpleSumsCrdtState of(float localSum, float otherSum) {
		return new SimpleSumsCrdtState(localSum, otherSum);
	}

	public float value() {
		return localSum + otherSum;
	}

	@Override
	public SimpleSumsCrdtState merge(SimpleSumsCrdtState state) {
		return new SimpleSumsCrdtState(Math.max(this.localSum, state.localSum), Math.max(this.otherSum, state.otherSum));
	}
}
