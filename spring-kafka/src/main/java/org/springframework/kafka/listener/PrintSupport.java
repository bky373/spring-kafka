package org.springframework.kafka.listener;

public class PrintSupport {

	public static String shortThreadName() {
		return Thread.currentThread()
				.getName()
				.replace("org.springframework.kafka.", "")
				.replace("-C-1", "");
	}

	public static String shortId(String id) {
		return id.replace("org.springframework.kafka.", "");
	}
}
