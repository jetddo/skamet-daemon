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

public class SHEAR_SGL_DataProcess extends DataProcessor {
	private static final String DATAFILE_PREFIX = "shear_sgl";
	private static final int DB_COLUMN_COUNT = 5;
	private static final int FILE_DATE_INDEX_POS = 0; // DFS_SHRT_STN_BEST_MERG_T1H_AIR.202005281200.csv (method overridden)
	private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // TM
	
	private final int INSERT_SGL_VECTOR_QUERY = 1;
	private final int INSERT_SGL_STN_QUERY = 2;
	
	public SHEAR_SGL_DataProcess(DaemonSettings settings){
		super(settings, DATAFILE_PREFIX);
	}
	
	protected List<SHEARData> parseFile(File file) throws IOException, ParseException {
		String line;
		String[] token;
		
		String stnCd = "RKPC";
		
		// Declare your column variables
		List<SHEARData> lstSHEAR = new ArrayList<SHEARData>();
		
		final SimpleDateFormat fctTmFormat = new SimpleDateFormat("yyyyMMddHH");

		try (BufferedReader br = new BufferedReader(new FileReader(file))){
			// HEADER 파싱
			Pattern tmPattern = Pattern.compile("^Forecast time [0-9]{2}");
			Pattern uvPattern = Pattern.compile("^U V");
			Pattern stnPattern = Pattern.compile("^Loss&Gain Lat_loc Lon_loc Station_1 Station_2");
			
			Date fctTm = null;
			Date tm = null;
			
			fctTm = fctTmFormat.parse(file.getName().split("_")[0]);
			
			while((line = br.readLine()) != null) {
				if(line.length() == 0) {
					continue;
				}
				
				if(tmPattern.matcher(line).find()) {
					Calendar cal = Calendar.getInstance();
					cal.setTime(fctTm);
					cal.add(Calendar.HOUR_OF_DAY, 9 + Integer.parseInt(line.split(" ")[2]));
					
					tm = cal.getTime();
					continue;
				}
				
				if(uvPattern.matcher(line).find()) {
					int key = 0;
					while((line = br.readLine()) != null) {
						
						if(stnPattern.matcher(line).find()) {
							break;
						} else if(line.length() == 0) {
							continue;
						}
						
						SHEARData shear = new SHEARData();
						
						shear.tm = tm;
						shear.fcstTm = fctTm;
						shear.stnCd = stnCd;
						shear.idx = key++;
						
						token = line.split(",");
						
						List<Object> item = new ArrayList<Object>();
						
						for(int i = 0; i < token.length; i++) {
							item.add(token[i]);
						}
						shear.list = item;
						lstSHEAR.add(shear);
					}
				} 
				
				if(stnPattern.matcher(line).find()) {
					int key = 0;
					while((line = br.readLine()) != null) {
						
						if(tmPattern.matcher(line).find()) {
							Calendar cal = Calendar.getInstance();
							cal.setTime(fctTm);
							cal.add(Calendar.HOUR_OF_DAY, 9 + Integer.parseInt(line.split(" ")[2]));
							
							tm = cal.getTime();
							
							break;
						} else if(line.length() == 0) {
							continue;
						}
						
						SHEARData shear = new SHEARData();
						
						shear.tm = tm;
						shear.fcstTm = fctTm;
						shear.stnCd = stnCd;
						shear.idx = key++;
						
						token = line.split(",");
						
						List<Object> item = new ArrayList<Object>();
						
						for(int i = 0; i < token.length; i++) {
							item.add(token[i]);
						}
						
						shear.list = item;
						
						lstSHEAR.add(shear);
					}
				}
			}
		}
		
		return lstSHEAR;
	}

	@Override
	protected void defineQueries() {
		// TODO Auto-generated method stub
		// 예측시간, 생산시간, 지점아이디, 지점코드, 기온
 		defineQueryFormat(INSERT_SGL_VECTOR_QUERY, "INSERT INTO AAMI.SHEAR_SGL_WIND(TM, FCT_TM, STN_CD, IDX, U, V, WS, WD) " 
		+ "VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'')");
 		defineQueryFormat(INSERT_SGL_STN_QUERY, "INSERT INTO AAMI.SHEAR_SGL_STN(TM, FCT_TM, STN_CD, IDX, LOSS_GAIN, LAT, LON, STN1, STN2) " 
 				+ "VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'')");
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
        	if(item.list != null && item.list.size() > 0) {
	        	String[] token = convertToRecordFormat(item);
	
	            // Insert to table
	        	if(token.length == 8) {
	        		query = MessageFormat.format(retrieveQueryFormat(INSERT_SGL_VECTOR_QUERY), (Object[]) token);
	        	} else {
	        		query = MessageFormat.format(retrieveQueryFormat(INSERT_SGL_STN_QUERY), (Object[]) token);
	        	}
	            dbManager.executeUpdate(query);
        	}
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
		
		int sz  = shear.list.size();
		for(int i = 0; i < sz; i++) {
			lstTokens.add(convertToDBText(shear.list.get(i)));
		}
		
		if(sz == 2) {
			Double u = Double.parseDouble(shear.list.get(0).toString());
			Double v = Double.parseDouble(shear.list.get(0).toString());

			double wd = Math.atan2(u, v) * 180 / Math.PI + 180;
			double ws = Math.sqrt(u*u + v*v) * 1.943844;
			
			lstTokens.add(convertToDBText(ws));
			lstTokens.add(convertToDBText(wd));
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
