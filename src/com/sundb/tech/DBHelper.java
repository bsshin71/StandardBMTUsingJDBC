package com.sundb.tech;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class DBHelper  {

	private Connection mConnection; 
	 
	static final String mInsertQuery = "INSERT INTO TEST01 (C1, C2, C3, C4, C5, C6, C7, C8, C9, C10) VALUES (?,?,?,?,?,?,?,?,?,SYSDATE)";
	static final String mSelectQuery = "SELECT * FROM TEST01 WHERE C1 = ? ";
	static final String mUpdateQuery = "UPDATE TEST01 SET C9 = C9+1 WHERE C1 = ? ";
	static final String mDeleteQuery = "DELETE FROM TEST01 WHERE C1 = ?;";
		
	private PreparedStatement mInsertStatement = null;
	private PreparedStatement mUpdateStatement = null;
	private PreparedStatement mSelectStatement = null;
	private PreparedStatement mDeleteStatement = null;

	public DBHelper ( String driverName,
					String url, 
					String user, 
					String password ) {
		
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			System.err.println ("can not load driver : " + driverName + "\n");
			System.exit(-1);
		
		}
		
		try {
			mConnection = DriverManager.getConnection(url, user, password );
		} catch (SQLException e) {
			System.err.println ("Can not connect....\n");
			e.printStackTrace();
			System.exit(-1);
		}
		if ( buildStatement () == false )  {
			System.err.println ("Can not prepare statements1....\n");
			System.exit(-1);
		}
		
	}
	 
	// Constructor 
	public DBHelper ( Connection aConnection) { 
		mConnection = aConnection ; 
		if ( buildStatement () == false )  {
			System.err.println ("Can not prepare statements2....\n");
			System.exit(-1);
		}
	}
	
	public void BeginTrans ( ) { 
		try {
			mConnection.setAutoCommit(false);
		} catch (SQLException e) {
			System.err.println("Transaction start failed ! ");
			System.exit( -1 );
		}
	}
	
	public void CommitTran ( ) {
		try {
			mConnection.commit();
		} catch (SQLException e) {
			System.err.println("Commit Failed ! ");
			System.exit( -1 );
		}
	}
	
	public void RollbackTran () {
		try {
			mConnection.rollback();
		} catch (SQLException e) {
			System.err.println("Rollback Failed ! ");
			System.exit( -1 );
		}
	}
	
	private boolean buildStatement ( ) {
		
		try {
			mInsertStatement = mConnection.prepareStatement(mInsertQuery);
			mUpdateStatement = mConnection.prepareStatement(mUpdateQuery);
			mSelectStatement = mConnection.prepareStatement(mSelectQuery);
			mDeleteStatement = mConnection.prepareStatement(mDeleteQuery);
		} catch (SQLException e) {
			System.err.println("Could not prepare statement .... ");
			e.printStackTrace();
			return false;
		}
		return true; 
	}
	
	public int Insert ( int aValue ) {
		
		String sStringValue = String.format("%010d", aValue);
		try {
			mInsertStatement.setInt( 1,  aValue );
			mInsertStatement.setString( 2,  sStringValue );
			mInsertStatement.setString( 3,  sStringValue );
			mInsertStatement.setString( 4,  sStringValue );
			mInsertStatement.setString( 5,  sStringValue );
			mInsertStatement.setString( 6,  sStringValue );
			mInsertStatement.setString( 7,  sStringValue );
			mInsertStatement.setString( 8,  sStringValue );
			mInsertStatement.setInt( 9,  aValue );
			
			if ( mInsertStatement.execute() == true ) { 
				return 0;
			};
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		
		return 0;
	}
	
	public int Update ( int aValue ) {
		try {
			mUpdateStatement.setInt( 1, aValue);
			if ( mUpdateStatement.execute() == true ) {
				return 0; 
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1; 
		}
		
		return -1;
	}
	
	public int Delete ( int aValue ) { 
		try {
			mDeleteStatement.setInt( 1, aValue);
			if ( mDeleteStatement.execute() == true ) {
				return 0; 
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1; 
		}
		
		return -1;
		
	}
	
	public int Select ( int aValue ) { 
		try {
			mSelectStatement.setInt( 1, aValue);
			ResultSet sResult = mSelectStatement.executeQuery();
			if ( sResult != null ) { 
				sResult.close();
				return 0 ; 
			} 
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1; 
		}
		
		return -1;
	}
}
