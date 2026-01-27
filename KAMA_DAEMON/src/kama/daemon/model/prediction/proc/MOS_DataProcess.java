package kama.daemon.model.prediction.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;
import kama.daemon.model.prediction.adopt.MOS.MOSData;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

public class MOS_DataProcess extends DataProcessor {
	private static final String DATAFILE_PREFIX = "mos";
	private static final int DB_COLUMN_COUNT = 5;
	private static final int FILE_DATE_INDEX_POS = 32; // DFS_SHRT_STN_BEST_MERG_T1H_AIR.202005281200.csv (method overridden)
	private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // TM
	private final int INSERT_QUERY = 1;
	private final int INSERT_AMIS_QUERY = 2;
	
	public MOS_DataProcess(DaemonSettings settings){
		super(settings, DATAFILE_PREFIX);
	}
	
	protected List<MOSData> parseFile(File file) throws IOException, ParseException {

		String line;
		String[] token;
		
		// Declare your column variables
		List<MOSData> lstMOS;
		
		lstMOS = new ArrayList<MOSData>();
		
		final SimpleDateFormat fctTmFormat = new SimpleDateFormat("yyyyMMddHH");

		try (BufferedReader br = new BufferedReader(new FileReader(file))){
			// HEADER 파싱
			Pattern tmPattern = Pattern.compile("^[0-9]{10}(UTC)*");
			Pattern hrPattern = Pattern.compile("^(STNID,)([\\s]*)[0-9]{1,2}(H,)");
			Pattern dataPattern = Pattern.compile("^[0-9]{5}(,)");
			
			Date fctTm = null;
			List<Integer> tmList = new ArrayList<Integer>();
			List<String[]> dataList = new ArrayList<String[]>();
			
			while((line = br.readLine()) != null) {
				
				if(tmPattern.matcher(line).find()) {
					fctTm = fctTmFormat.parse(line.substring(0, 10));
				} else if(hrPattern.matcher(line).find()) {
					token = line.replaceAll("H", "").split(",");
					for(int i = 1; i < token.length; i++) {
						tmList.add(Integer.parseInt(token[i].trim()));
					}
				} else if(dataPattern.matcher(line).find()) {
					token = line.split(",");
					dataList.add(token);
				}
			}
			
			for(int i = 0; i < dataList.size(); i++) {
				token = dataList.get(i);
				
				for(int j = 1; j < token.length; j++) {
					MOSData mos = new MOSData();
					
					Calendar cal = Calendar.getInstance();
					cal.setTime(fctTm);
					cal.add(Calendar.HOUR_OF_DAY, tmList.get(j-1) + 9);
					
					mos.tm = cal.getTime();
					mos.fcstTm = fctTm;
					mos.stnId = Integer.parseInt(token[0].substring(2));
					mos.tmp = (("-").equals(token[0]) ? Float.NaN : Float.parseFloat(token[j]));
					
					// mos.dir = (stnList.containsKey(token[0].substring(2)) ? stnList.get(token[0].substring(2)) : null);
					
					lstMOS.add(mos);
				}
			}
		}
		
		return lstMOS;
		
	}

	@Override
	protected void defineQueries() {
		// TODO Auto-generated method stub
		// 예측시간, 생산시간, 지점아이디, 지점코드, 기온
 		defineQueryFormat(INSERT_QUERY, "INSERT INTO AMIS.FCT_MOS(TM, FCT_TM, STN_ID, MOS_NOW) " 
		+ "VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), ''{2}'', ''{3}'')");
 		defineQueryFormat(INSERT_AMIS_QUERY, "INSERT INTO fly.FCT_MOS(TM, FCT_TM, STN_ID, MOS_NOW) " 
 				+ "VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), ''{2}'', ''{3}'')");
	}

	/**
     * 각 (하나의) 파일에 대한 데이터 처리 함수
     * @param dbManager 데이터베이스 매니저
     * @param file 처리할 데이터 파일
     * @param processorInfo 처리할 데이터에 대한 부가적인 정보가 담긴 구조체
     * @throws Exception
     */
    @SuppressWarnings("serial")
	@Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        List<MOSData> lstMOS;
        String query;
        
        lstMOS = parseFile(file);
        
        for(MOSData item : lstMOS) {
        	String[] token = convertToRecordFormat(item);

            // Insert to table
            query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), (Object[]) token);
            dbManager.executeUpdate(query);
        }
        
        // 현재 입력된  MOS 자료에 3시간 전 MOS 자료 업데이트
        if(lstMOS.size() > 0) {
        	final SimpleDateFormat fctTmFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        	
	        query = "MERGE INTO AMIS.FCT_MOS mosNow "
					+ "USING ( "
					+ "SELECT TM + 3/24 TM, FCT_TM, STN_ID, MOS_NOW "
					+ "FROM AMIS.FCT_MOS "
					+ "WHERE FCT_TM = TO_DATE('''" + fctTmFormat.format(lstMOS.get(0).fcstTm) + "''', '''yyyy-MM-dd HH24:mi''') ) mos3h "
					+ "ON (mosNow.STN_ID=mos3h.STN_ID AND mosNow.TM = mos3h.TM AND mosNow.FCT_TM = mos3h.FCT_TM) "
					+ "WHEN MATCHED THEN "
					+ "UPDATE "
					+ "SET mosNow.MOS_3H = mos3h.MOS_NOW";
	        
	        dbManager.executeUpdate(query);
        }
        
        /* AMIS MOS *********************************************************/
        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
		encryptor.setPassword("pwkey");
        
        try {
	        Configurations configs = new Configurations();
			Configuration config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
	        String urlAMIS = "jdbc:oracle:thin:@192.168.0.128:1521:aami";
	    	String idAMIS = "amis";
	    	String pwAMIS = "koast3369";
	    	
	    	urlAMIS = config.getString("db.amis.url");
			idAMIS = config.getString("db.amis.user");
			pwAMIS = encryptor.decrypt(config.getString("db.amis.password"));
	    	
			List<String> insertQueryList = new ArrayList<String>();
	        for(MOSData item : lstMOS) {
	        	String[] token = convertToRecordFormat(item);
	
	            // Insert to table
	            query = MessageFormat.format(retrieveQueryFormat(INSERT_AMIS_QUERY), (Object[]) token);
	            insertQueryList.add(query);
	            System.out.println("query: " + query);
	        }
	        updateQuery(insertQueryList, urlAMIS, idAMIS, pwAMIS);
	        
	        // 현재 입력된  MOS 자료에 3시간 전 MOS 자료 업데이트
	        if(lstMOS.size() > 0) {
	        	final SimpleDateFormat fctTmFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	        	
		        final String amisUpdateQuery = "MERGE INTO fly.FCT_MOS mosNow "
						+ "USING ( "
						+ "SELECT TM + 3/24 TM, FCT_TM, STN_ID, MOS_NOW "
						+ "FROM fly.FCT_MOS "
						+ "WHERE FCT_TM = TO_DATE('''" + fctTmFormat.format(lstMOS.get(0).fcstTm) + "''', '''yyyy-MM-dd HH24:mi''') ) mos3h "
						+ "ON (mosNow.STN_ID=mos3h.STN_ID AND mosNow.TM = mos3h.TM AND mosNow.FCT_TM = mos3h.FCT_TM) "
						+ "WHEN MATCHED THEN "
						+ "UPDATE "
						+ "SET mosNow.MOS_3H = mos3h.MOS_NOW";
		        
		        updateQuery(new ArrayList<String>(){{add(amisUpdateQuery);}}, urlAMIS, idAMIS, pwAMIS);
	        }
        } catch(Exception err) {
        	err.printStackTrace();
        }
    }
	
	private String[] convertToRecordFormat(MOSData mos) {
		List<String> lstTokens;
		String[] tokens;
		
		lstTokens = new ArrayList<String>();

		lstTokens.add(convertToDBText(mos.tm));
		lstTokens.add(convertToDBText(mos.fcstTm));
		lstTokens.add(convertToDBText(mos.stnId));
		lstTokens.add(convertToDBText(mos.tmp));
		lstTokens.add(convertToDBText(mos.tmp));
		
		tokens = new String[lstTokens.size()];
		
		return lstTokens.toArray(tokens);
	}

	@Override
	protected int getDateFileIndex() {
		// TODO Auto-generated method stub
		return FILE_DATE_INDEX_POS;
	}
	
	private void updateQuery(List<String> list, String urlAAMI, String idAAMI, String pwAAMI) {
		Connection _conn = null;
		Statement _stmt = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			
			_conn = DriverManager.getConnection(urlAAMI, idAAMI, pwAAMI);
			_stmt = _conn.createStatement();
			
			for(int i = 0, sz=list.size(); i < sz; i++) {
				try {
					int rst = _stmt.executeUpdate(list.get(i));
					// System.out.println("query: " + list.get(i));
					// System.out.println("Result Code: " + rst);
				} catch (Exception e) {
					// System.out.println("query: " + list.get(i));
					e.printStackTrace();
				}
			}
			
			_stmt.close();
			_conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
