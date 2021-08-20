package io.activej.codegen;

import java.io.IOException;
import java.util.Optional;

public interface BytecodeStorage {
	Optional<byte[]> loadBytecode(String className) throws IOException;

	void saveBytecode(String className, byte[] bytecode) throws IOException;
}
