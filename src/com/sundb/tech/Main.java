package com.sundb.tech;

public class Main {

	public static void main(String[] args) {
	
		/*
		Runner sRunner = new Runner ("sunje.sundb.jdbc.SundbDriver", 
										   "jdbc:sundb://192.168.0.48:22581/test", 
										   "TEST", 
										   "test" );
        */
		
		Runner sRunner = new Runner ("cubrid.jdbc.driver.CUBRIDDriver", 
				   "jdbc:cubrid:192.168.0.48:33000:testdb:::", 
				   "DBA", 
				   "" );
		
		sRunner.setRange(1, 10000);
		sRunner.setOperationType(Runner.INSERT_OPERATION);
		
		sRunner.run();
		
		sRunner.setOperationType(Runner.SELECT_OPERATION);
		sRunner.run() ;
		
		sRunner.setOperationType(Runner.UPDATE_OPERATION);
		sRunner.run() ;
		
		sRunner.setOperationType(Runner.DELETE_OPERATION);
		sRunner.run() ;
		
		
		sRunner.setOperationType(Runner.COMPLEX_OPERATION);
		sRunner.run() ;
		
		
		
	}

}
