package de.tu_berlin.dima.bigdata.matrixfactorization;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class log4jTest{

	private static final Logger logger = Logger.getLogger(log4jTest.class.getName()); 
	
	public static void main(String [] args){

		 PropertyConfigurator.configure("log4j.properties");
         logger.info("Applcaiton Starts");
         logger.warn("Bar Starts");
         logger.error("Bar Errors");
         logger.warn("Bar  Exits");
         logger.info("Foo Starts");
         logger.error("Foo Errors");
         logger.debug("Foo exits ");
         logger.info("Applcaition Exits");
	}
	
}