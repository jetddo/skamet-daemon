package kama.daemon.model.prediction.proc;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import oracle.jdbc.OraclePreparedStatement;

public class RTKO66_DataProcess extends DataProcessor {
	
	private static final String DATAFILE_PREFIX = "rtko66";
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public RTKO66_DataProcess(final DaemonSettings settings) {
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
	
	private void parseRTKO66File(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start RTKO66 Parser :::::");
		
		try {

            Connection conn = dbManager.getConnection();
            
            String tcId = file.getName().split("\\]")[1].split("\\.")[0];
            String issuedDt = file.getName().split("\\_")[1].split("\\]")[0];
            
            String query = "INSERT INTO RTKO66(TC_ID, ISSUED_DT, DOMAIN_15, DOMAIN_25, DOMAIN_70, DOMAIN_NORMAL_15, DOMAIN_NORMAL_25, DOMAIN_NORMAL_70)"  
                    	+ "VALUES (?, TO_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?, ?, ?, ?)"; 
            
            OraclePreparedStatement ops = (OraclePreparedStatement)conn.prepareStatement(query.toString());
            
	    	LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream(file.getAbsoluteFile()), "EUC-KR"));
	    	
	    	String[] domainList = new String[]{"15", "25", "70", "15 normal", "25 normal", "70 normal"};
	    	String[] domainCoordList = new String[]{"", "", "", "", "", ""};
	    	
	    	int currentDomainIndex = 0;
	    	int currentCoordCount = 0;
	    	int coordSkip = 1;
	    	
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
	    			System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) 
	    					+ " -> '" + domainList[currentDomainIndex] + "' coordinates length : " + domainCoordList[currentDomainIndex].split("\\|").length);	    			
	    			continue;
	    		}
	    		
	    		try {
	    			
	    			currentCoordCount++;
	    	
	    			if(currentCoordCount % coordSkip == 0) {
	    				domainCoordList[currentDomainIndex] += line + "|";	
	    			}
	    			
	    		} catch (Exception e) {}
	    	}
		
	    	ops.setString(1, tcId);
	    	ops.setString(2, issuedDt);
	    	
	    	for(int i=0 ; i<domainCoordList.length ; i++) {
	    		ops.setStringForClob(i+3, domainCoordList[i].toString());	
	    	}
	    	
	    	ops.execute();
	    	
	    	ops.close();
	    	reader.close();	 
	    	
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End RTKO66 Parser :::::");
	}

	@Override
	protected void processDataInternal(DatabaseManager dbManager, File file,
			ProcessorInfo processorInfo) throws Exception {
			
		this.parseRTKO66File(dbManager, file, processorInfo);  	
		
	}
}
