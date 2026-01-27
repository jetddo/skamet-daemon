package kama.daemon.main;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class UpdateTafInpNm {
    	
	public void parseRTKO66File(String radarFilePath) {
	
		Connection _conn = null;
		Statement stmt = null;
			
		try {
			
			 Class.forName("oracle.jdbc.driver.OracleDriver");

            // open database
            _conn = DriverManager.getConnection("jdbc:oracle:thin:@172.26.56.110:1521:aami", "aami", "HKGUWypLr1");
            
            _conn.setAutoCommit(false);
	        
            stmt = _conn.createStatement();
            
            LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream(radarFilePath), "UTF-8"));
	     	
            String line = null;
            
            while((line = reader.readLine()) != null) {
            	
            	if(reader.getLineNumber()==1){continue;}
            	
            	String[] tokens = line.replaceAll("\"","").split(",");
            	
            	String tafTmStr = tokens[0];
            	String stnCd = tokens[1];
            	String tafKind = tokens[2];
            	String inpNm = tokens[3];
            	
            	String query = "UPDATE EVAL_TAF_RESULT SET INP_NM = '" + inpNm + "' WHERE EVAL_TM = TO_DATE('"+tafTmStr+"','YYYYMMDDHH24MI') "+
            	" AND FCST_KIND = '"+tafKind+"' AND STN_CD = '"+stnCd+"'";
            	
            	stmt.executeUpdate(query);
            	
            	if(reader.getLineNumber()%100==0){
            		System.out.println(reader.getLineNumber());
            	}
            }
	    	reader.close();
            
	    	_conn.commit();
		
		} catch (Exception e) {
			e.printStackTrace();
			
			try {
				
				_conn.rollback();
				
			} catch (Exception e1) {
				
			}
			
			
		} finally {
			
			try {
				
				stmt.close();
		    	_conn.close();
		    	
			} catch (Exception e) {
				
			}
		}
	}
	
	public static void main(String[] args) {
		
		UpdateTafInpNm radarTest = new UpdateTafInpNm();
		
		String[] aaa = new String[]{
			"rkny","rkjy","rkss","rkpc","rkjb","rkpu","rkpk","rktu","rktn","rkjj","rkth","rkps"
		};
		for(String aa:aaa) {
			radarTest.parseRTKO66File("C:/Users/koast/Desktop/taf_update/"+aa+".csv");	
		}
		
	}

}
