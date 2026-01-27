package kama.daemon.model.prediction.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.model.observation.adopt.AMOS.AMOS_Dict;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chlee
 * Created on 2016-11-28.
 */
public class FCT_KLAPS_10M_DataProcess extends DataProcessor
{
	private static final String DATAFILE_PREFIX = "fct_klaps_10m";
    private static final int DB_COLUMN_COUNT = 24;
    private static final int FILE_DATE_INDEX_POS = 32; // ieodo_20190904.dat (method overridden)
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // TM
    private final int INSERT_QUERY = 1;
	
	public FCT_KLAPS_10M_DataProcess(DaemonSettings settings) {
		super(settings, DATAFILE_PREFIX);
		// TODO Auto-generated constructor stub
	}
	
	private List<Map<String, String>> parseKlapsFile(File file, String stnId, String issuedTm) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		List<Map<String, String>> klapsDataList = new ArrayList<Map<String, String>>();
		
		try {
			
			BufferedReader br = new BufferedReader(new FileReader(file.getAbsoluteFile()));
				
			Calendar cal = new GregorianCalendar();
			
			String line = null;
			
			for (int i=0 ; (line = br.readLine()) != null; i++) {
				
				if(i == 0) {
					continue;
				}
				
				Map<String, String> map = new HashMap<String, String>();
				
				String[] tokens = line.split(",");
					
				map.put("stnId", stnId);
				map.put("issuedTm", issuedTm);
				map.put("fcstTm", tokens[1]);
				map.put("rain", tokens[2]);
				
				klapsDataList.add(map);
			}
			
			br.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return klapsDataList;
	}
	
	private boolean insertKlapsDataList(List<Map<String, String>> klapsDataList, DatabaseManager dbManager) {
		
		if(klapsDataList.size() < 1) {
			return false;
		}
		
		for(int i=0 ; i<klapsDataList.size() ; i++) {
			
			Map<String, String> klapsData = klapsDataList.get(i);
			
			String insertQuery = "INSERT INTO AAMI.FCT_KLAPS_10M (ISSUED_TM, FCST_TM, STN_ID, RAIN) "+
					 "VALUES (TO_DATE('"+klapsData.get("issuedTm")+"', 'YYYYMMDDHH24MI'), TO_DATE('"+klapsData.get("fcstTm")+"', 'YYYYMMDDHH24MI'), '"+klapsData.get("stnId") +"', '"+klapsData.get("rain")+"')";
			
			
			dbManager.executeUpdate(insertQuery);
		}
		
		return true;
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
						
		try {

			String stnId = file.getName().split("\\.")[0].split("_")[2];
			
			String issuedTm = file.getName().split("\\.")[1];
			
			List<Map<String, String>> klapsDataList = this.parseKlapsFile(file, stnId, issuedTm);
			
			this.insertKlapsDataList(klapsDataList, dbManager);
			
		} catch (Exception e) {
			e.printStackTrace();
			dbManager.rollback();
		}
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