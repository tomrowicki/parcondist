package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;
import edu.rice.pcdp.PCDP;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 *
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determin the number of primes <= limit.
 */
public final class SieveActor extends Sieve {

	/**
	 * {@inheritDoc}
	 *
	 * TODO Use the SieveActorActor class to calculate the number of primes <=
	 * limit in parallel. You might consider how you can model the Sieve of
	 * Eratosthenes as a pipeline of actors, each corresponding to a single
	 * prime number.
	 */
	@Override
	public int countPrimes(final int limit) {
		int numPrimes = 1;
		SieveActorActor sieveActor = new SieveActorActor();
		PCDP.finish(() -> {
			for (int i = 3; i <= limit; i += 2) {
				sieveActor.send(i);
			}
		});
		SieveActorActor loopActor = sieveActor;
		while (loopActor != null) {
			numPrimes += loopActor.numLocalPrimes();
			loopActor = loopActor.nextActor();
		}
		return numPrimes;
	}

	/**
	 * An actor class that helps implement the Sieve of Eratosthenes in
	 * parallel.
	 */
	public static final class SieveActorActor extends Actor {

		/**
		 * Process a single message sent to this actor.
		 *
		 * TODO complete this method.
		 *
		 * @param msg Received message
		 */
		private static final int MAX_LOCAL_PRIMES = 10_000;
		private final int localPrimes[] = new int[MAX_LOCAL_PRIMES];
		private int numLocalPrimes = 1;
		private SieveActorActor nextActor;

		public SieveActorActor nextActor() {
			return nextActor;
		}

		public int numLocalPrimes() {
			return numLocalPrimes;
		}

		@Override
		public void process(final Object msg) {
			int candidate = (int) msg;
			localPrimes[0] = candidate;
			final boolean locallyPrime = isLocallyPrime(candidate);
			if (locallyPrime) {
				if (numLocalPrimes < MAX_LOCAL_PRIMES) {
					localPrimes[numLocalPrimes] = candidate;
					numLocalPrimes += 1;
				} else if (nextActor == null) {
					nextActor = new SieveActorActor();
				} else {
					nextActor.send(msg);
				}
			}
		}

		private boolean isLocallyPrime(final int candidate) {
			final boolean[] isPrime = {
					true
			};
			checkPrimeKernel(candidate, isPrime, 0, numLocalPrimes);
			return isPrime[0];
		}

		private void checkPrimeKernel(int candidate, boolean[] isPrime, int startIndex, int endIndex) {
			for (int i = startIndex; i < endIndex; i++) {
				if (candidate % localPrimes[i] == 0) {
					isPrime[0] = false;
				}
			}
		}
	}
}
