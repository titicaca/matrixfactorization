package de.tu_berlin.dima.bigdata.matrixfactorization.util;

import java.util.regex.Pattern;

import com.google.common.primitives.Longs;

import eu.stratosphere.pact.client.LocalExecutor;
import eu.stratosphere.pact.common.plan.Plan;


public class Util{
	public static final int USER_ID_POS = 0;
	public static final int ITEM_ID_POS = 1;
	public static final int RATING_POS = 2;

	/** Standard delimiter of textual preference data */
	private static final Pattern PREFERENCE_TOKEN_DELIMITER = Pattern
			.compile("[\t,]");

	private Util() {
	}

	/**
	 * Splits a preference data line into string tokens
	 */
	public static String[] splitPrefTokens(CharSequence line) {
		return PREFERENCE_TOKEN_DELIMITER.split(line);
	}

	/**
	 * Maps a long to an int
	 */
	public static int idToIndex(long id) {
		return 0x7FFFFFFF & Longs.hashCode(id);
	}
	
	public static int readID(String token) {
		return Integer.parseInt(token);
	}
	
	public static float readRate(String token){
		return Float.parseFloat(token);
	}

	public static int readID(String token, boolean usesLongIDs) {
		return usesLongIDs ? idToIndex(Long.parseLong(token)) : Integer
				.parseInt(token);
	}
	
	public static void executePlan(Plan toExecute) throws Exception {
		LocalExecutor executor = new LocalExecutor();
		executor.start();
		long runtime = executor.executePlan(toExecute);
		System.out.println("runtime:  " + runtime);
		executor.stop();
	}
}