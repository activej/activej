package io.activej.fs.exception;

import org.jetbrains.annotations.NotNull;

import java.util.Map;

import static java.util.stream.Collectors.joining;

public final class FsBatchException extends FsStateException {
	private final Map<String, FsScalarException> exceptions;

	public FsBatchException(@NotNull Class<?> component, @NotNull Map<String, FsScalarException> exceptions) {
		super(component, "Operation failed");
		this.exceptions = exceptions;
	}

	public Map<String, FsScalarException> getExceptions() {
		return exceptions;
	}

	@Override
	public String getMessage() {
		return super.getMessage() + " : exceptions=" + exceptions.entrySet().stream()
				.limit(10)
				.map(element -> element.getKey() + '=' + element.getValue().getMessage())
				.collect(joining(",", "{", exceptions.size() <= 10 ? "}" : ", ..and " + (exceptions.size() - 10) + " more}"));
	}
}
