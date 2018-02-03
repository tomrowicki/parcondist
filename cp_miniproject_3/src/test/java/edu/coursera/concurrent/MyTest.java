package edu.coursera.concurrent;

import junit.framework.TestCase;

public class MyTest extends TestCase {

	public void testFor10() {
		SieveActor sieveActor = new SieveActor();
		int outcome = sieveActor.countPrimes(10);
		System.out.println("test outcome: " + outcome);
		assertTrue(outcome == 4);
	}
}
