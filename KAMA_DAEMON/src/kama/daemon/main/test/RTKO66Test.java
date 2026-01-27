package kama.daemon.main.test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import oracle.jdbc.OraclePreparedStatement;

public class RTKO66Test {

	
	public void parseRTKO66File(String radarFilePath) {
		
		Connection _conn = null;
		Statement _stmt = null;
			
		try {
			
			 Class.forName("oracle.jdbc.driver.OracleDriver");

            // open database
            _conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.0.128:1521:aami", "aami", "koast3369");
            _stmt = _conn.createStatement();
            
            _conn.setAutoCommit(false);
	            
            String query = "INSERT INTO RTKO66(TC_ID, ISSUED_DT, DOMAIN_15, DOMAIN_25, DOMAIN_70, DOMAIN_NORMAL_15, DOMAIN_NORMAL_25, DOMAIN_NORMAL_70)"  
                    	+ "VALUES (1, sysdate, ?, ?, ?, ?, ?, ?)"; 
            
            OraclePreparedStatement ops = (OraclePreparedStatement)_conn.prepareStatement(query.toString());
            
	    	LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream("F:/data/typoon_test.txt")));
	    	
	    	String[] domainList = new String[]{"15", "25", "70", "15 normal", "25 normal", "70 normal"};
	    	String[] domainCoordList = new String[]{"", "", "", "", "", ""};
	    	
	    	int currentDomainIndex = 0;
	    	int currentCoordCount = 0;
	    	int coordSkip = 50;
	    	
	    	String line = "";
	    	
	    	while ((line = reader.readLine()) != null) {
	    		
	    		line = line.trim();
	    		
	    		if(line.contains("start")) {
	    			
	    			for(int i=0 ; i<domainList.length ; i++) {
	    				
	    				if(line.contains(domainList[i])) {
	    					currentDomainIndex = i;
	    				}
	    			}
	    			
	    			currentCoordCount = 0;
	    			
	    			continue;
	    		}
	    		
	    		if(line.contains("end")) {	    			
	    			continue;
	    		}
	    		
	    		try {
	    			
	    			currentCoordCount++;
	    	
	    			if(currentCoordCount % coordSkip == 0) {
	    				domainCoordList[currentDomainIndex] += line + "|";	
	    			}
	    			
	    		} catch (Exception e) {}
	    	}
		
	    	for(int i=0 ; i<domainCoordList.length ; i++) {
	    		ops.setStringForClob(i+1, domainCoordList[i].toString());	
	    	}
	    	
	    	ops.execute();
	    	
	    	//_conn.commit();
	    	
	    	
	    	reader.close();
	    	_stmt.close();
	    	_conn.close();
	
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	
	}
	
	public static void main(String[] args) {
		
		RTKO66Test radarTest = new RTKO66Test();
		
		radarTest.parseRTKO66File("F:/KAMA_AAMI/2019/RTKO63/RTKO66_201911281600]28.txt");
	}

}
