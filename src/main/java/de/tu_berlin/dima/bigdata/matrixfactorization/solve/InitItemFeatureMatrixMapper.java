package de.tu_berlin.dima.bigdata.matrixfactorization.solve;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * input averageRating :
 * 	key: 0
 *  value: Vector { userid : average rating} 
 *  
 * output Initial Item Feature Matrix
 * 	key: itemID
 * 	value: Vector { featureID : value}
 * @author titicaca
 *
 */
public class InitItemFeatureMatrixMapper extends MapStub{

	private final PactRecord outputRecord = new PactRecord();
	
	@Override
	public void map(PactRecord record, Collector<PactRecord> collector)
			throws Exception {
		// TODO Auto-generated method stub
		
	}
	
}