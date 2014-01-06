package de.tu_berlin.dima.bigdata.matrixfactorization;

import de.tu_berlin.dima.bigdata.matrixfactorization.itemrating.ItemRatingVectorMapper;
import de.tu_berlin.dima.bigdata.matrixfactorization.itemrating.ItemRatingVectorReducer;
import de.tu_berlin.dima.bigdata.matrixfactorization.prediction.PredictionCrosser;
import de.tu_berlin.dima.bigdata.matrixfactorization.solve.FeatureMatrixUpdatePlan;
import de.tu_berlin.dima.bigdata.matrixfactorization.solve.InitItemFeatureMatrixMapper;
import de.tu_berlin.dima.bigdata.matrixfactorization.solve.ItemFeatureMatrixCrosser;
import de.tu_berlin.dima.bigdata.matrixfactorization.solve.ItemFeatureMatrixReducer;
import de.tu_berlin.dima.bigdata.matrixfactorization.solve.UserFeatureMatrixCrosser;
import de.tu_berlin.dima.bigdata.matrixfactorization.solve.UserFeatureMatrixReducer;
import de.tu_berlin.dima.bigdata.matrixfactorization.type.PactVector;
import de.tu_berlin.dima.bigdata.matrixfactorization.userrating.UserRatingVectorMapper;
import de.tu_berlin.dima.bigdata.matrixfactorization.userrating.UserRatingVectorReducer;
import de.tu_berlin.dima.bigdata.matrixfactorization.util.Util;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactFloat;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.generic.contract.IterationContract;

public class MatrixFactorizationPlan implements PlanAssembler, PlanAssemblerDescription{
	  
	private final int numIterations = 20;
	
	private final CrossContract userFeatureMatrixCrossers[] = new CrossContract[numIterations];
	private final CrossContract itemFeatureMatrixCrossers[] = new CrossContract[numIterations];
	private final ReduceContract userFeatureMatrixReducers[] = new ReduceContract[numIterations];
	private final ReduceContract itemFeatureMatrixReducers[] = new ReduceContract[numIterations];
	
	@Override
	public String getDescription() {
		return "Usage: [inputPath] [outputPath] ([numSubtasks])";
	}
	@Override
	public Plan getPlan(String... args) {
		String inputPath = args.length >= 1 ? args[0] : "";
		String outputPath = args.length >= 2 ? args[1] : "";
		int numSubtasks = args.length >= 3 ? Integer.parseInt(args[2]) : 1;

		FileDataSource source = new FileDataSource(new TextInputFormat(), inputPath, "Input Documents");
		
		System.out.println("Processing.. start iteration..");
		
		MapContract itemRatingVectorMapper = MapContract
				.builder(ItemRatingVectorMapper.class).input(source)
				.name("Item Rating Vector Mapper").build();

		ReduceContract itemRatingVectorReducer = ReduceContract
				.builder(ItemRatingVectorReducer.class, PactInteger.class, 0)
				.input(itemRatingVectorMapper).name("Item Rating Vector Reducer").build();
		
		MapContract userRatingVectorMapper = MapContract
				.builder(UserRatingVectorMapper.class).input(source)
				.name("User Rating Vector Mapper").build();

		ReduceContract userRatingVectorReducer = ReduceContract
				.builder(UserRatingVectorReducer.class, PactInteger.class, 0)
				.input(userRatingVectorMapper).name("User Rating Vector Reducer").build();
		
		MapContract initItemFeatureMatrixMapper = MapContract
				.builder(InitItemFeatureMatrixMapper.class).input(itemRatingVectorReducer)
				.name("init Item Feature Matrix Mapper").build();
		
		ReduceContract initItemFeatureMatrixReducer = ReduceContract
				.builder(ItemFeatureMatrixReducer.class, PactInteger.class, 0)
				.input(initItemFeatureMatrixMapper).name("init Item Feature Matrix Reducer").build();
		
		userFeatureMatrixCrossers[0] = CrossContract
				.builder(UserFeatureMatrixCrosser.class).input1(userRatingVectorReducer).input2(initItemFeatureMatrixReducer)
				.name("User Feature Matrix Update Crosser 1").build();
		
		userFeatureMatrixReducers[0] = ReduceContract
				.builder(UserFeatureMatrixReducer.class, PactInteger.class, 0).input(userFeatureMatrixCrossers[0])
				.name("User Feature Matrix Update Reducer 1").build();
		
		for(int i = 1; i < numIterations; i ++){
//			System.out.println("iteration :" + i);
			itemFeatureMatrixCrossers[i-1] = CrossContract
					.builder(ItemFeatureMatrixCrosser.class).input1(itemRatingVectorReducer).input2(userFeatureMatrixReducers[i-1])
					.name("Item Feature Matrix Update Crosser " + (i)).build();
			
			itemFeatureMatrixReducers[i-1] = ReduceContract
					.builder(ItemFeatureMatrixReducer.class, PactInteger.class, 0).input(itemFeatureMatrixCrossers[i-1])
					.name("Item Feature Matrix Update Reducer " + (i)).build();
			
			userFeatureMatrixCrossers[i] = CrossContract
					.builder(UserFeatureMatrixCrosser.class).input1(userRatingVectorReducer).input2(itemFeatureMatrixReducers[i-1])
					.name("User Feature Matrix Update Crosser " + (i+1)).build();
			
			userFeatureMatrixReducers[i] = ReduceContract
					.builder(UserFeatureMatrixReducer.class, PactInteger.class, 0).input(userFeatureMatrixCrossers[i])
					.name("User Feature Matrix Update Reducer " + (i+1)).build();
		}
		
		itemFeatureMatrixCrossers[numIterations-1] = CrossContract
				.builder(ItemFeatureMatrixCrosser.class).input1(itemRatingVectorReducer).input2(userFeatureMatrixReducers[numIterations-1])
				.name("Item Feature Matrix Update Crosser " + (numIterations)).build();
		
		itemFeatureMatrixReducers[numIterations-1] = ReduceContract
				.builder(ItemFeatureMatrixReducer.class, PactInteger.class, 0).input(itemFeatureMatrixCrossers[numIterations-1])
				.name("Item Feature Matrix Update Reducer " + (numIterations)).build();	
		
		CrossContract predictCrosser = CrossContract.builder(PredictionCrosser.class)
				.input1(itemFeatureMatrixCrossers[numIterations-1])
				.input2(userFeatureMatrixCrossers[numIterations-1])
				.name("Predict Crosser")
				.build();
		
		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, outputPath, predictCrosser, "Rating Prediction");
		RecordOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactInteger.class, 0)
			.field(PactInteger.class, 1)
			.field(PactFloat.class, 2);
		

		Plan plan = new Plan(sink, "Rating Prediction Computation");
		plan.setDefaultParallelism(numSubtasks);

		return plan;
	}
	public static void main(String[] args) throws Exception {

//		String inputPath = "file://"+System.getProperty("user.dir") +"/datasets/10m/r1.train";
//
//		String outputPath = "file://"+System.getProperty("user.dir") +"/results/10m/Prediction__r1_i=2";
		
		String inputPath = "file://"+System.getProperty("user.dir") +"/datasets/100k/ua.base.txt";

		String outputPath = "file://"+System.getProperty("user.dir") +"/results/100k/Prediction_ua_i=20.result";


		System.out.println("Reading input from " + inputPath);
		System.out.println("Writing output to " + outputPath);

		Plan toExecute = new MatrixFactorizationPlan().getPlan(inputPath, outputPath);
//		toExecute.setDefaultParallelism(1);
		Util.executePlan(toExecute);
		
		// Util.deleteAllTempFiles();
	}
	
}