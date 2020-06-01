package io.activej.remotefs.stress;

import java.io.IOException;

public class StressTestAllOperations {
	// first should start server form StressServer, then call main as many times as you like
	public static void main(String[] args) throws IOException {
		StressClient client = new StressClient();
		client.setup();
		client.start(100, 360_000);
	}
}
