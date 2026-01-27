package kama.daemon.model.prediction.proc;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import oracle.jdbc.OraclePreparedStatement;

public class RTKO632_DataProcess extends DataProcessor {
	
	private static final String DATAFILE_PREFIX = "rtko63";
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private final int RTKO63INFO_LENGTH = 22;
	private final int RTKO63INFO_HEADER_LENGTH = 7;
	private final int RTKO63FCST_LENGTH = 15;

    private String rtko63InfoQuery = "INSERT INTO RTKO63_INFO(STN_ID, TM_FC, TYP_SEQ, TM_SEQ, "
			+ "TYP_NAME, TYP_EN, MAN_FC, TYP_TM, TYP_LAT, TYP_LON, TYP_LOC, "
			+ "TYP_DIR, TYP_SP, TYP_PS, TYP_WS, TYP_TP, TYP_25, TYP_25ED, TYP_25ER, TYP_15, TYP_15ED, TYP_15ER)"  
        	+ "VALUES (?, TO_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?, ?, ?, TO_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private String rtko63FcstQuery = "INSERT INTO RTKO63_FCST(STN_ID, TM_FC, TYP_SEQ, TM_SEQ, "
    			+ "TYP_NAME, TYP_EN, MAN_FC, FT_TM, FT_LAT, FT_LON, FT_LOC, "
    			+ "FT_PS, FT_WS, FT_RAD, FT_15, FT_15ED, FT_15ER, FT_25, FT_25ED, FT_25ER, FT_DIR, FT_SP)"  
            	+ "VALUES (?, TO_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?, ?, ?, TO_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	public RTKO632_DataProcess(final DaemonSettings settings) {
		super(settings, DATAFILE_PREFIX);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void defineQueries() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected int getDateFileIndex() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	private void parseRTKO63File(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start RTKO63 Parser :::::");
		
		try {

			boolean flag = true;
            Connection conn = dbManager.getConnection();
            dbManager.setAutoCommit(false);
            
            LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream(file.getAbsoluteFile()), "EUC-KR"));
	     	
	    	String line = reader.readLine().trim();
	    	
	    	String[] tokens = line.split("#", -1);
	    	
	    	String[] rtko63InfoHeader = Arrays.copyOfRange(tokens, 0, RTKO63INFO_HEADER_LENGTH);	    	
	    	String[] rtko63Info = Arrays.copyOfRange(tokens, 0 , RTKO63INFO_LENGTH);
	    	
	    	if(this.validateArray(rtko63Info, 16)) {
	       	
	    		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> RTKO63 Header : ");
	    		
	    		for(String s : rtko63InfoHeader) {
	    			System.out.print(s + "\t");
	    		}
	    		
	    		System.out.println();
	    		
	    		this.insertRTKO63(conn, this.rtko63InfoQuery, rtko63Info);
	    		
	    		for(int i=0 ; i<10 ; i++) {
	    			
	    			String[] rtko63Fcst = Arrays.copyOfRange(tokens, RTKO63INFO_LENGTH + RTKO63FCST_LENGTH * i, RTKO63INFO_LENGTH + RTKO63FCST_LENGTH * (i + 1));
	    			
		    		List<String> rtKo63FcstList = new ArrayList<String>();
		    	    		
		    		rtKo63FcstList.addAll(Arrays.asList(rtko63InfoHeader));
		    		rtKo63FcstList.addAll(Arrays.asList(rtko63Fcst));
		    			
		    		rtko63Fcst = rtKo63FcstList.toArray(new String[rtKo63FcstList.size()]);
		    		
	    			if(this.validateArray(rtko63Fcst, 13)) {
	    				
	    				System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> Fcst Index : " + (i+1));
	    				
	    				try {
	    					
	    					this.insertRTKO63(conn, this.rtko63FcstQuery, rtko63Fcst);
	    					
	    				} catch (SQLException e) {	    				
	    					dbManager.rollback();
	    					flag = false;
	    					break;
	    				}    				
	    			}
	    		}
	    	}
	    	
	    	reader.close();
			
	    	if(flag) {
	    		dbManager.commit();	
	    	}
	    	
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End RTKO63 Parser :::::");
	}
	
	private void insertRTKO63(Connection conn, String query, String[] params) throws SQLException {
		
		try {
			
			OraclePreparedStatement ops = (OraclePreparedStatement)conn.prepareStatement(query.toString());
			 
			for(int i=0 ; i<params.length ; i++) {
				ops.setString(i+1, params[i]);
			}
			 
			ops.execute();
			ops.close();
			 
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
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
	
	private boolean validateArray(String[] array, int length) {
		
		for(int i=0 ; i<length ; i++) {
			
			if(array[i] == null || "".equals(array[i])) {
				return false;
			}
		}
		
		return true;
	}

	@Override
	protected void processDataInternal(DatabaseManager dbManager, File file,
			ProcessorInfo processorInfo) throws Exception {
			
		this.parseRTKO63File(dbManager, file, processorInfo);  	
		
	}
}
