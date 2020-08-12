package io.activej.fs.stress;

import java.io.IOException;

public class StressTestUploadSmallObjects {
	public static void main(String[] args) throws IOException {
		StressClient client = new StressClient();
		client.setup();
		for (int i = 0; i < 100; i++) {
			client.uploadSerializedObject();
		}
		for (int i = 0; i < 100; i++) {
			client.downloadSmallObjects(i);
		}
	}
}
