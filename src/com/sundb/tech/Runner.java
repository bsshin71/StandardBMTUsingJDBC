package com.sundb.tech;

import java.util.Collections;
import java.util.Vector;

public class Runner implements Runnable {

	private DBHelper mDB = null; 
	private int mStartIndex, mLastIndex;
	private int mOpType;
	
	private Vector<Long> mVector = new Vector<Long>(); 
	

	public static final int INSERT_OPERATION  = 1;
	public static final int UPDATE_OPERATION  = 2;
	public static final int DELETE_OPERATION  = 3;
	public static final int SELECT_OPERATION  = 4;
	public static final int COMPLEX_OPERATION = 5;
	
	
			
	public Runner (String driverName,
				   String url, 
				   String user, 
				   String password) {
		mDB = new DBHelper ( driverName, url, user, password ) ; 	
	}


	public void setRange ( int aStartIndex, int aLastIndex ) { 
		mStartIndex = aStartIndex; 
		mLastIndex = aLastIndex; 
	}
	
	public void setOperationType ( int aOpType ) { 
		mOpType = aOpType;
	}
	
	public void run() {
		
		long sStartTime,  sEndTime; 
		for ( int i = mStartIndex ; i < mLastIndex + 1 ; i ++ ) {
			sStartTime = System.nanoTime();			
			
			switch ( mOpType ) { 
			case INSERT_OPERATION :
				mDB.Insert(i);
				break; 
			case UPDATE_OPERATION :
				mDB.Update(i);
				break;
			case DELETE_OPERATION :
				mDB.Delete(i);
				break;
			case SELECT_OPERATION :
				mDB.Select( i );
				break;
			case COMPLEX_OPERATION :
				mDB.BeginTrans();
				mDB.Insert(i);
				mDB.Update(i);
				mDB.Select(i);
				mDB.Delete(i);
				mDB.CommitTran();
				break; 
			}
			sEndTime = System.nanoTime();
			mVector.add ( (sEndTime - sStartTime) / 1000);
		}
		
		// print statistics record end of loop
		printStatisticsRecord () ;
	}


	private void printStatisticsRecord() {
		Collections.sort(mVector);
		long sSum = 0 ;
		double sAvg = 0;
		for ( int i = 0 ; i < mVector.size() ; i ++ ) {
			sSum += mVector.get(i);
		}
		sAvg = sSum / mVector.size();
		
		int sPct99Index = mVector.size() - ( mVector.size() / 100) + 1 ; 
		int sPct999Index = mVector.size() - ( mVector.size() / 1000) + 1;
		
		double sSec = sSum / 1000000;
		double sRPS = mVector.size() / sSec;

		System.out.print( "TPS : " + sRPS + "  ");
		System.out.println("Min : " + mVector.get(0)   + " MAX :" + mVector.get(mVector.size()-1) + " AVG : " + sAvg + " 99% : " + mVector.get( sPct99Index)+ " 99.9% : " + mVector.get( sPct999Index)      );
	}
	
	
	
}
