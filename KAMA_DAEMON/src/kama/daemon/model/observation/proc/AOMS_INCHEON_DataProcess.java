package kama.daemon.model.observation.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import kama.daemon.common.db.AmisDataBaseManager;
import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;

public class AOMS_INCHEON_DataProcess extends DataProcessor {

	private static final String DATAFILE_PREFIX = "aoms_incheon";
    private static final int DB_COLUMN_COUNT = 24;
    private static final int FILE_DATE_INDEX_POS = 32; // ieodo_20190904.dat (method overridden)
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // TM
    private final int INSERT_QUERY = 1;
    
    private AmisDataBaseManager amisDbmanager;
    
    private boolean amisDbInitialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			Configuration config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.amisDbmanager = new AmisDataBaseManager(config);
			this.amisDbmanager.setAutoCommit(false);
			
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : AOMS_INCHEON_DataProcess.amisDbinitialize -> " + e);
			
			this.amisDbmanager.safeClose();
			
			return false;
		}
		
		return true;
	}
    
	private void amisDbDestroy() {
		
		this.amisDbmanager.safeClose();
	}
	
	public AOMS_INCHEON_DataProcess(DaemonSettings settings) {
		super(settings, DATAFILE_PREFIX);
		// TODO Auto-generated constructor stub
		
		this.insertHistory = false;
	}
	
	private String[] parseFile(File file) throws IOException, ParseException {
		
		Object[][] columnMetaInfo = new Object[][]{
				
				{"TM_STMP",18},
				{"MS_DATE",8},
				{"MSG_TYPE",1},
				{"MS_IO",1},
				{"FLIGHT_NUM",8},
				{"MS_SID",1},
				{"MS_AIRP",3},
				{"MS_AIRP_FN",30},
				{"MS_STIME",4},
				{"MS_CTIME",4},
				{"MS_LINE",1},
				{"MS_ATIME",4},
				{"MS_DELAY",1},
				{"MS_CAUSE",2},
				{"MS_DECITY",3},
				{"MS_PAX",1},
				{"MG_REASON_AIRPORT",1},
				{"MS_CODE",4},
				{"MG_USECODE",1},
				{"MS_PURPO",2},
				{"REF1",10},
				{"REF2",10},
				{"REF3",10},
				{"REF4",10},
				{"REF5",10}
		};
		
		int cursor = 36;
		
		List<String> valueList = new ArrayList<String>();
		
		try (BufferedReader br = new BufferedReader(new FileReader(file.getAbsoluteFile()))){
			
			String line = br.readLine();
			
			if(line.length() != 184) {
				System.out.println("Error : AOMS_INCHEON_DataProcess.parseFile -> line length is" + line.length());
			} else {
				line += " ";
			}
			
			for(Object[] columnInfo : columnMetaInfo) {
				
				String columnName = columnInfo[0].toString();
				int columnLength = Integer.valueOf(columnInfo[1].toString());
				
				String token = line.substring(cursor, cursor+columnLength);
				cursor += columnLength;
				
				valueList.add(token.trim());
			}			
		}
		
		return valueList.toArray(new String[valueList.size()]);
	}
	
	private String buildQuery(String[] valueList) {
		
		String query = "INSERT INTO AAMI.AOMS VALUES (";
		
		for(int i=0 ; i<valueList.length ; i++) {
			
			String value = valueList[i];
			
			if(i == 0) {				
				query += "TO_TIMESTAMP('" + value + "','YYYYMMDDHH24MISS.FF3')";
			} else {
				query += "'" + value + "'";
			}
			
			if(i < valueList.length-1) {
				query += ",";
			}
		}
		
		query += ")";
		
		return query;
	}
	
	private String buildAmisDbQuery(String[] valueList) {
		
		String query = "INSERT INTO KAMAWEB.AOMS VALUES (";
		
		for(int i=0 ; i<valueList.length ; i++) {
			
			String value = valueList[i];
			
			if(i == 0) {				
				query += "TO_TIMESTAMP('" + value + "','YYYYMMDDHH24MISS.FF3')";
			} else {
				query += "'" + value + "'";
			}
			
			if(i < valueList.length-1) {
				query += ",";
			}
		}
		
		query += ")";
		
		return query;
	}

	/**
     * 각 (하나의) 파일에 대한 데이터 처리 함수
     * @param dbManager 데이터베이스 매니저
     * @param file 처리할 데이터 파일
     * @param processorInfo 처리할 데이터에 대한 부가적인 정보가 담긴 구조체
     * @throws Exception
     */
    @Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
    	
    	String[] valueList = this.parseFile(file);

    	String query = this.buildQuery(valueList);
    	
    	dbManager.executeQuery(query);
    	
    	this.amisDbInitialize();
    	
    	String amisQuery = this.buildAmisDbQuery(valueList);
    	
    	this.amisDbmanager.insert(amisQuery);
    	this.amisDbmanager.commit();
    	
    	this.amisDbDestroy();
    }
	
	@Override
	protected int getDateFileIndex() {
		// TODO Auto-generated method stub
		return FILE_DATE_INDEX_POS;
	}

	@Override
	protected void defineQueries() {
		// TODO Auto-generated method stub
		
	}

}
