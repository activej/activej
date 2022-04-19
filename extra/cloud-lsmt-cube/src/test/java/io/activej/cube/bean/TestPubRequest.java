package io.activej.cube.bean;

import io.activej.serializer.annotations.Serialize;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.util.Arrays.asList;

public class TestPubRequest {
	private final static Random rand = new Random();

	public enum TestEnum { ONE, TWO}

	public static class TestAdvRequest {
		@Serialize
		public int adv;

		public TestAdvRequest() {
		}

		public TestAdvRequest(int adv) {
			this.adv = adv;
		}

		@Override
		public String toString() {
			return "TestAdvRequest{adv=" + adv + '}';
		}
	}

	@Serialize
	public long timestamp;
	@Serialize
	public int pub;

	@Serialize
	public List<TestAdvRequest> advRequests = new ArrayList<>();

	public TestPubRequest() {
	}

	public TestPubRequest(int timestamp, int pub, List<TestAdvRequest> advRequests) {
		this.timestamp = timestamp;
		this.pub = pub;
		this.advRequests = advRequests;
	}

	public static int randInt(int min, int max) {
		return rand.nextInt((max - min) + 1) + min;
	}

	public static TestPubRequest randomPubRequest() {
		TestPubRequest randomRequest = new TestPubRequest();
		randomRequest.timestamp = randInt(1, 10_000);
		randomRequest.pub = randInt(1, 10_000);

		List<TestAdvRequest> advRequests = new ArrayList<>();
		for (int i = 0; i < randInt(1, 20); ++i) {
			advRequests.add(new TestAdvRequest(randInt(1, 10_000)));
		}
		randomRequest.advRequests = advRequests;

		return randomRequest;
	}

	@Override
	public String toString() {
		return "TestPubRequest{timestamp=" + timestamp + ", pub=" + pub + ", advRequests=" + advRequests + '}';
	}

	public static final List<String> DIMENSIONS = asList("adv", "pub");

	public static final List<String> METRICS = asList("advRequests", "pubRequests");
}
