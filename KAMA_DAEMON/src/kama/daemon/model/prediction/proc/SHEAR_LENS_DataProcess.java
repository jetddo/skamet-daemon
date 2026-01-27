package kama.daemon.model.prediction.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.main.DaemonMain;
import kama.daemon.model.prediction.adopt.SHEAR.SHEARData;
import kama.daemon.common.util.DateFormatter;

public class SHEAR_LENS_DataProcess extends DataProcessor {
	private static final String DATAFILE_PREFIX = "shear_lens";
	private static final int DB_COLUMN_COUNT = 5;
	private static final int FILE_DATE_INDEX_POS = 0; // DFS_SHRT_STN_BEST_MERG_T1H_AIR.202005281200.csv (method overridden)
	private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // TM
	
	private final int INSERT_QUERY = 1;
	
	public SHEAR_LENS_DataProcess(DaemonSettings settings){
		super(settings, DATAFILE_PREFIX);
	}
	
	protected List<SHEARData> parseFile(File file) throws IOException, ParseException {
		String line;
		String[] token;
		
		// Declare your column variables
		List<SHEARData> lstSHEAR = new ArrayList<SHEARData>();
		
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
					SHEARData shear = new SHEARData();
					
					Calendar cal = Calendar.getInstance();
					cal.setTime(fctTm);
					cal.add(Calendar.HOUR_OF_DAY, tmList.get(j-1) + 9);
					
					shear.tm = cal.getTime();
					shear.fcstTm = fctTm;
					shear.stnCd = "RKPC";
					// shear.tmp = (("-").equals(token[0]) ? Float.NaN : Float.parseFloat(token[j]));
					
					// shear.dir = (stnList.containsKey(token[0].substring(2)) ? stnList.get(token[0].substring(2)) : null);
					
					lstSHEAR.add(shear);
				}
			}
		}
		
		return lstSHEAR;
	}

	@Override
	protected void defineQueries() {
		// TODO Auto-generated method stub
		// 예측시간, 생산시간, 지점아이디, 지점코드, 기온
 		defineQueryFormat(INSERT_QUERY, "INSERT INTO AMIS.FCT_MOS(TM, FCT_TM, STN_ID, MOS_NOW) " 
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
        List<SHEARData> lstSHEAR;
        String query;
        
        lstSHEAR = parseFile(file);
        
        if(lstSHEAR == null) return;
        
        /*
        for(SHEARData item : lstSHEAR) {
        	String[] token = convertToRecordFormat(item);

            // Insert to table
            query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), (Object[]) token);
            dbManager.executeUpdate(query);
        }
        */
    }
	
	private String[] convertToRecordFormat(SHEARData shear) {
		List<String> lstTokens;
		String[] tokens;
		
		lstTokens = new ArrayList<String>();

		lstTokens.add(convertToDBText(shear.tm));
		lstTokens.add(convertToDBText(shear.fcstTm));
		lstTokens.add(convertToDBText(shear.stnCd));
		// lstTokens.add(convertToDBText(shear.));
		
		tokens = new String[lstTokens.size()];
		
		return lstTokens.toArray(tokens);
	}

	@Override
	protected int getDateFileIndex() {
		// TODO Auto-generated method stub
		return FILE_DATE_INDEX_POS;
	}
}
