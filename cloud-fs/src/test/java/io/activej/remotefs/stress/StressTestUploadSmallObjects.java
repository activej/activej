package io.activej.remotefs.stress;

import java.io.IOException;

public class StressTestUploadSmallObjects {
	public static void main(String[] args) throws IOException {
		StressClient client = new StressClient();
		client.setup();
		for (int i = 0; i < 100; i++) {
			client.uploadSerializedObject(i);
		}
		for (int i = 0; i < 100; i++) {
			client.downloadSmallObjects(i);
		}
	}
}
