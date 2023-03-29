package io.activej.eventloop.schedule;

import java.util.Arrays;

import static io.activej.eventloop.schedule.ScheduledRunnable.compare;

public class ScheduledPriorityQueue {
	private static final int DEFAULT_INITIAL_CAPACITY = 16;

	private ScheduledRunnable[] queue;
	private int size;

	public ScheduledPriorityQueue() {
		this.queue = new ScheduledRunnable[DEFAULT_INITIAL_CAPACITY];
	}

	public int size() {
		return size;
	}

	public boolean isEmpty() {
		return size == 0;
	}

	public void add(ScheduledRunnable e) {
		if (size >= queue.length) {
			queue = Arrays.copyOf(queue, queue.length * 2);
		}
		siftUp(queue, size, e);
		size++;
	}

	public ScheduledRunnable peek() {
		return queue[0];
	}

	public ScheduledRunnable poll() {
		ScheduledRunnable result = queue[0];
		if (result != null) {
			size--;
			ScheduledRunnable x = queue[size];
			queue[size] = null;
			if (size > 0) {
				siftDown(queue, 0, x, size);
			}
		}
		return result;
	}

	public void remove(ScheduledRunnable e) {
		int i = e.index;
		size--;
		if (size == i) {
			queue[i] = null;
		} else {
			ScheduledRunnable x = queue[size];
			queue[size] = null;
			siftDown(queue, i, x, size);
			if (queue[i] == x) {
				siftUp(queue, i, x);
			}
		}
	}

	private static void siftUp(ScheduledRunnable[] queue, int k, ScheduledRunnable x) {
		while (k > 0) {
			int parent = (k - 1) >>> 1;
			ScheduledRunnable t = queue[parent];
			if (compare(x, t) >= 0) break;
			set(queue, k, t);
			k = parent;
		}
		set(queue, k, x);
	}

	private static void siftDown(ScheduledRunnable[] queue, int k, ScheduledRunnable x, int n) {
		int half = n >>> 1;
		while (k < half) {
			int child = (k << 1) + 1;
			ScheduledRunnable t = queue[child];
			int right = child + 1;
			if (right < n && compare(t, queue[right]) > 0) {
				t = queue[child = right];
			}
			if (compare(x, t) <= 0) break;
			set(queue, k, t);
			k = child;
		}
		set(queue, k, x);
	}

	private static void set(ScheduledRunnable[] entries, int index, ScheduledRunnable entry) {
		entries[index] = entry;
		entry.index = index;
	}

}
