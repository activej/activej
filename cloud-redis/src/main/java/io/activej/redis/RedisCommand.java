package io.activej.redis;

import java.nio.charset.Charset;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class RedisCommand {
	private final Command command;
	private final List<byte[]> arguments;

	private RedisCommand(Command command, List<byte[]> arguments) {
		this.command = command;
		this.arguments = arguments;
	}

	public static RedisCommand of(Command command, Charset charset, List<String> arguments) {
		return new RedisCommand(command, arguments.stream().map(s -> s.getBytes(charset)).collect(toList()));
	}

	public static RedisCommand of(Command command, Charset charset, String... arguments) {
		return RedisCommand.of(command, charset, asList(arguments));
	}

	public static RedisCommand of(Command command, List<byte[]> arguments) {
		return new RedisCommand(command, arguments);
	}

	public static RedisCommand of(Command command, byte[]... arguments) {
		return new RedisCommand(command, asList(arguments));
	}

	public Command getCommand() {
		return command;
	}

	public List<byte[]> getArguments() {
		return arguments;
	}

	@Override
	public String toString() {
		return "'" + command + ' ' + arguments.stream().map(String::new).collect(joining(" ")) + '\'';
	}
}
