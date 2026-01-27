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

public class SHEAR_ISOB_DataProcess extends DataProcessor {
	private static final String DATAFILE_PREFIX = "shear_isob";
	private static final int DB_COLUMN_COUNT = 5;
	private static final int FILE_DATE_INDEX_POS = 0; // DFS_SHRT_STN_BEST_MERG_T1H_AIR.202005281200.csv (method overridden)
	private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // TM
	
	private final int INSERT_QUERY = 1;
	
	public SHEAR_ISOB_DataProcess(DaemonSettings settings){
		super(settings, DATAFILE_PREFIX);
	}
	
	protected List<SHEARData> parseFile(File file) throws IOException, ParseException {
		String line;
		String[] token;
		
		// Declare your column variables
		List<SHEARData> lstSHEAR = new ArrayList<SHEARData>();
		
		final SimpleDateFormat fctTmFormat = new SimpleDateFormat("yyyyMMddHH");
		
		String[] file_name = file.getName().split("_|\\+|\\.");
		
		try (BufferedReader br = new BufferedReader(new FileReader(file))){
			Date tm = null;
			Date fctTm = null;
			String stnCd = "RKPC";
			String rwy_dir = "";
			String state = "";
			
			fctTm = fctTmFormat.parse(file_name[2]);
			Calendar cal = Calendar.getInstance();
			cal.setTime(fctTm);
			cal.add(Calendar.HOUR_OF_DAY, 9 + Integer.parseInt(file_name[3].replace("F", "")));
			tm = cal.getTime();
			
			rwy_dir = file_name[0].substring(0, 2);
			state = file_name[0].substring(2);
			
			List<String[]> dataList = new ArrayList<String[]>();
			
			while((line = br.readLine()) != null) {
				if(line.length() > 0) {
					token = line.split(" ");
					dataList.add(token);
				}
			}
			
			for(int i = 0; i < dataList.size(); i++) {
				token = dataList.get(i);
				
				SHEARData shear = new SHEARData();
				
				shear.tm = tm;
				shear.fcstTm = fctTm;
				shear.stnCd = stnCd;
								
				shear.idx = i;
				
				List<Object> item = new ArrayList<Object>();
				item.add(rwy_dir);
				item.add(state);
				
				for(int j = 0; j < token.length; j++) {
					item.add(token[j]);
				}
				
				shear.list = item;
				
				lstSHEAR.add(shear);
			}
		}
		
		return lstSHEAR;
	}

	@Override
	protected void defineQueries() {
		// TODO Auto-generated method stub
		// 예측시간, 생산시간, 지점아이디, 지점코드, 기온
 		defineQueryFormat(INSERT_QUERY, "INSERT INTO AAMI.SHEAR_ISOB(TM, FCT_TM, STN_CD, IDX, RWY_DIR, STATE, DISTANCE, VAL) " 
 				+ "VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'')");
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
        
        for(SHEARData item : lstSHEAR) {
        	String[] token = convertToRecordFormat(item);

            // Insert to table
            query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), (Object[]) token);
            dbManager.executeUpdate(query);
        }
    }
	
	private String[] convertToRecordFormat(SHEARData shear) {
		List<String> lstTokens;
		String[] tokens;
		
		lstTokens = new ArrayList<String>();

		lstTokens.add(convertToDBText(shear.tm));
		lstTokens.add(convertToDBText(shear.fcstTm));
		lstTokens.add(convertToDBText(shear.stnCd));
		lstTokens.add(convertToDBText(shear.idx));
		
		int sz = shear.list.size();
		for(int i = 0; i < sz; i++) {
			lstTokens.add(convertToDBText(shear.list.get(i)));
		}
		
		tokens = new String[lstTokens.size()];
		
		return lstTokens.toArray(tokens);
	}

	@Override
	protected int getDateFileIndex() {
		// TODO Auto-generated method stub
		return FILE_DATE_INDEX_POS;
	}
}
