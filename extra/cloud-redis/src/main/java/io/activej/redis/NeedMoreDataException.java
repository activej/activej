package io.activej.redis;

// Local marker exception for requesting more data
public final class NeedMoreDataException extends RuntimeException {
	static final NeedMoreDataException NEED_MORE_DATA = new NeedMoreDataException();

	private NeedMoreDataException() {
		super();
	}

	@Override
	public final Throwable fillInStackTrace() {
		return this;
	}
}
