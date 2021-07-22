package adder;

import io.activej.crdt.primitives.CrdtMergable;

public final class SimpleSumsCrdtState implements CrdtMergable<SimpleSumsCrdtState> {
	private final float localSum;
	private final float otherSum;

	private SimpleSumsCrdtState(float localSum, float otherSum) {
		this.localSum = localSum;
		this.otherSum = otherSum;
	}

	public static SimpleSumsCrdtState of(float localSum) {
		return new SimpleSumsCrdtState(localSum, 0);
	}

	public static SimpleSumsCrdtState of(float localSum, float otherSum) {
		return new SimpleSumsCrdtState(localSum, otherSum);
	}

	public float value() {
		return localSum + otherSum;
	}

	public float getLocalSum() {
		return localSum;
	}

	public float getOtherSum() {
		return otherSum;
	}

	@Override
	public SimpleSumsCrdtState merge(SimpleSumsCrdtState state) {
		return new SimpleSumsCrdtState(Math.max(this.localSum, state.localSum), Math.max(this.otherSum, state.otherSum));
	}
}
