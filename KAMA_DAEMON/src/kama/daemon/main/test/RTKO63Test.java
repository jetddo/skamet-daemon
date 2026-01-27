package kama.daemon.main.test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration2.sync.SynchronizerSupport;

import oracle.jdbc.OraclePreparedStatement;

public class RTKO63Test {
	
	private final int RTKO63INFO_LENGTH = 22;
	private final int RTKO63INFO_HEADER_LENGTH = 7;
	private final int RTKO63FCST_LENGTH = 12;

    private String rtko63InfoQuery = "INSERT INTO RTKO63_INFO(STN_ID, TM_FC, TYP_SEQ, TM_SEQ, "
			+ "TYP_NAME, TYP_EN, MAN_FC, TYP_TM, TYP_LAT, TYP_LON, TYP_LOC, "
			+ "TYP_DIR, TYP_SP, TYP_PS, TYP_WS, TYP_TP, TYP_25, TYP_25ED, TYP_25ER, TYP_15, TYP_15ED, TYP_15ER)"  
        	+ "VALUES (?, TO_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?, ?, ?, TO_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private String rtko63FcstQuery = "INSERT INTO RTKO63_FCST(STN_ID, TM_FC, TYP_SEQ, TM_SEQ, "
    			+ "TYP_NAME, TYP_EN, MAN_FC, FT_TM, FT_LAT, FT_LON, FT_LOC, "
    			+ "FT_PS, FT_WS, FT_RAD, FT_15, FT_15ED, FT_15ER, FT_DIR, FT_SP)"  
            	+ "VALUES (?, TO_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?, ?, ?, TO_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    	
	public void parseRTKO66File(String radarFilePath) {
	
		Connection _conn = null;
			
		try {
			
			 Class.forName("oracle.jdbc.driver.OracleDriver");

            // open database
//            _conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.0.128:1521:aami", "aami", "koast3369");
//            
//            _conn.setAutoCommit(false);
	        
            LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream(radarFilePath), "EUC-KR"));
	     	
	    	String line = reader.readLine().trim();
	    	
	    	String[] tokens = line.split("#", -1);
	    	
	    	String[] rtko63InfoHeader = Arrays.copyOfRange(tokens, 0, RTKO63INFO_HEADER_LENGTH);	
	    	String[] rtko63Info = Arrays.copyOfRange(tokens, 0 , RTKO63INFO_LENGTH);
	    	
	    	for(String s : rtko63Info) {
	    		System.out.println(s);	
	    	}
	    	
	    	
	    	if(this.validateArray(rtko63Info)) {
	    		
//	    		this.insertRTKO63(_conn, this.rtko63InfoQuery, rtko63Info);
	    		
	    		for(int i=0 ; i<10 ; i++) {
	    			
	    			String[] rtko63Fcst = Arrays.copyOfRange(tokens, RTKO63INFO_LENGTH + RTKO63FCST_LENGTH * i, RTKO63INFO_LENGTH + RTKO63FCST_LENGTH * (i + 1));
	    			
		    		List<String> rtKo63FcstList = new ArrayList<String>();
		    		rtKo63FcstList.addAll(Arrays.asList(rtko63InfoHeader));
		    		rtKo63FcstList.addAll(Arrays.asList(rtko63Fcst));
		    		
		    		rtko63Fcst = rtKo63FcstList.toArray(new String[rtKo63FcstList.size()]);
		    		
	    			if(this.validateArray(rtko63Fcst)) {
	    				this.insertRTKO63(_conn, this.rtko63FcstQuery, rtko63Fcst);	    				
	    			}
	    		}
	    	}
	    	
//	    	_conn.commit();
	    	
	    	reader.close();
//	    	_conn.close();
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void insertRTKO63(Connection conn, String query, String[] params) {
		
		try {
			
			OraclePreparedStatement ops = (OraclePreparedStatement)conn.prepareStatement(query.toString());
			 
			for(int i=0 ; i<params.length ; i++) {
				ops.setString(i+1, params[i]);
			}
			 
			ops.execute();
			ops.close();
			 
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private boolean validateArray(String[] array) {
		
		for(String s : array) {
			
			if(s == null || "".equals(s)) {
				return false;
			}
		}
		
		return true;
	}
	
	public static void main(String[] args) {
		
		RTKO63Test radarTest = new RTKO63Test();
		
		radarTest.parseRTKO66File("C:/Users/koast/Desktop/asdfasdf/FTP_AIR/RTKO63_201902212200]02");
	}

}
