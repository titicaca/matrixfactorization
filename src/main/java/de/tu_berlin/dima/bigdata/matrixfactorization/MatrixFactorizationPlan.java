package de.tu_berlin.dima.bigdata.matrixfactorization;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class MatrixFactorizationPlan implements PlanAssembler, PlanAssemblerDescription{
	
	private boolean implicitFeedback;
	private int numIterations;
	private int numFeatures;
	private double lambda;
	private double alpha;
//	private int numThreadsPerSolver;
	private boolean usesLongIDs; 

	private int numItems;
	private int numUsers;
	  
	
	@Override
	public String getDescription() {
		return "Usage: [inputPath] [outputPath] ([numSubtasks])";
	}

	@Override
	public Plan getPlan(String... args) {
//		String inputPath = args.length >= 1 ? args[0] : "";
//		String outputPath = args.length >= 2 ? args[1] : "";
//		int numSubtasks = args.length >= 3 ? Integer.parseInt(args[2]) : 1;
//
//		FileDataSource source = new FileDataSource(new TextInputFormat(), inputPath, "Input Documents");
//
//		
//		
//		
//		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, outputPath, dfReducer, "Predicted Matrix");
//		RecordOutputFormat.configureRecordFormat(sink)
//			.recordDelimiter('\n')
//			.fieldDelimiter(' ')
//			.field(PactString.class, 0) // term
//			.field(PactInteger.class, 1); // document frequency
//
//		Plan plan = new Plan(sink, "Matrix Factorization");
//		plan.setDefaultParallelism(numSubtasks);
//
//		return plan;
		return null;
	}
	
}