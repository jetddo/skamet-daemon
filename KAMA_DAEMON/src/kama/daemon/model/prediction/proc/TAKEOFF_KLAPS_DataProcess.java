package kama.daemon.model.prediction.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;

public class TAKEOFF_KLAPS_DataProcess extends DataProcessor {

	private static final String DATAFILE_PREFIX = "takeoff_klaps";
    private static final int DB_COLUMN_COUNT = 24;
    private static final int FILE_DATE_INDEX_POS = 32; // ieodo_20190904.dat (method overridden)
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // TM
    private final int INSERT_QUERY = 1;
	
	public TAKEOFF_KLAPS_DataProcess(DaemonSettings settings) {
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
				
				int h = Integer.valueOf(tokens[0]);
				cal.setTime(sdf.parse(issuedTm));
				cal.add(Calendar.HOUR_OF_DAY, h);
				
				String klapsTmp = tokens[5];
				String klapsQnh = "" + (Float.valueOf(tokens[2])*0.029525*100);
				String klapsWd = tokens[3];
				String klapsWs = "" + (Float.valueOf(tokens[4])*1.943844f);
				String klapsQnhHpa = "" + Float.valueOf(tokens[2]);
				
				map.put("stnId", stnId);
				map.put("issuedTm", issuedTm);
				map.put("fcstTm", sdf.format(cal.getTime()));
				map.put("klapsTmp", klapsTmp);
				map.put("klapsQnh", klapsQnh);
				map.put("klapsWd", klapsWd);
				map.put("klapsWs", klapsWs);
				map.put("klapsQnhHpa", klapsQnhHpa);
				
				klapsDataList.add(map);
			}
			
			br.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return klapsDataList;
	}
	
	private boolean insertKlapsDataList(List<Map<String, String>> klapsDataList, DatabaseManager dbManager) {
		
		String insertQuery = "INSERT INTO AMIS.FCT_KLAPS (ISSUED_TM, FCST_TM, STN_ID, KLAPS_TMP, KLAPS_QNH, KLAPS_WD, KLAPS_WS, KLAPS_QNH_HPA) "+
							 "VALUES (TO_DATE(''{0}'', ''YYYYMMDDHH24''), TO_DATE(''{1}'', ''YYYYMMDDHH24''), ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'')";
		
		if(klapsDataList.size() < 1) {
			return false;
		}
		
		for(int i=0 ; i<klapsDataList.size() ; i++) {
			
			Map<String, String> klapsData = klapsDataList.get(i);
			
			String query = MessageFormat.format(insertQuery, new Object[]{
				
					klapsData.get("issuedTm"),
					klapsData.get("fcstTm"),
					klapsData.get("stnId"),
					klapsData.get("klapsTmp"),
					klapsData.get("klapsQnh"),
					klapsData.get("klapsWd"),
					klapsData.get("klapsWs"),
					klapsData.get("klapsQnhHpa")
			});
			
			dbManager.executeUpdate(query);
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
		
		String updateQuery = "UPDATE AMIS.FCT_KLAPS A SET "+
			" A.KLAPS_TMP_3H = (SELECT KLAPS_TMP FROM AMIS.FCT_KLAPS WHERE STN_ID = ''{0}'' AND ISSUED_TM = TO_DATE(''{1}'', ''YYYYMMDDHH24'') AND FCST_TM = A.FCST_TM - 3/24), "+
			" A.KLAPS_QNH_3H = (SELECT KLAPS_QNH FROM AMIS.FCT_KLAPS WHERE STN_ID = ''{2}'' AND ISSUED_TM = TO_DATE(''{3}'', ''YYYYMMDDHH24'') AND FCST_TM = A.FCST_TM - 3/24), "+
			" A.KLAPS_WD_3H = (SELECT KLAPS_WD FROM AMIS.FCT_KLAPS WHERE STN_ID = ''{4}'' AND ISSUED_TM = TO_DATE(''{5}'', ''YYYYMMDDHH24'') AND FCST_TM = A.FCST_TM - 3/24), "+
			" A.KLAPS_WS_3H = (SELECT KLAPS_WS FROM AMIS.FCT_KLAPS WHERE STN_ID = ''{6}'' AND ISSUED_TM = TO_DATE(''{7}'', ''YYYYMMDDHH24'') AND FCST_TM = A.FCST_TM - 3/24) "+
			" WHERE A.STN_ID = ''{8}'' AND A.ISSUED_TM = TO_DATE(''{9}'', ''YYYYMMDDHH24'') ";
						
		try {

			String stnId = file.getName().split("_")[2];
			
			String issuedTm = file.getName().split("_")[3].split("\\.")[0];
			
			List<Map<String, String>> klapsDataList = this.parseKlapsFile(file, stnId, issuedTm);
			
			if(this.insertKlapsDataList(klapsDataList, dbManager)) {
					
				dbManager.executeUpdate(MessageFormat.format(updateQuery, new Object[]{
					
					stnId,
					issuedTm,
					stnId,
					issuedTm,
					stnId,
					issuedTm,
					stnId,
					issuedTm,
					stnId,
					issuedTm
				}));
			}
			
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
