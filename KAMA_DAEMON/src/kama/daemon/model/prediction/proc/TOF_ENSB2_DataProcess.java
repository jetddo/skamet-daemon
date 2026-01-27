package kama.daemon.model.prediction.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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

/**
 * @author chlee
 * Created on 2016-11-28.
 */
public class TOF_ENSB2_DataProcess extends DataProcessor
{
	private static final String DATAFILE_PREFIX = "tof_ensb2";
    private static final int DB_COLUMN_COUNT = 24;
    private static final int FILE_DATE_INDEX_POS = 32; // ieodo_20190904.dat (method overridden)
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // TM
    private final int INSERT_QUERY = 1;
	
	public TOF_ENSB2_DataProcess(DaemonSettings settings) {
		super(settings, DATAFILE_PREFIX);
		// TODO Auto-generated constructor stub
	}
	
	private List<Map<String, String>> parseTofEnsb2File(File file, String stnCd, String issuedTm) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		List<Map<String, String>> tofEnsb2DataList = new ArrayList<Map<String, String>>();
		
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
				
				String fcstTm = tokens[3];
				String wdir = tokens[4];
				String wspd = tokens[5];
				String temp = tokens[6];
				String pres = tokens[7];
				
				map.put("issuedTm", issuedTm);
				map.put("fcstTm", fcstTm);
				map.put("stnCd", stnCd);
				map.put("wdir", wdir);
				map.put("wspd", wspd);
				map.put("temp", temp);
				map.put("pres", pres);
				
				tofEnsb2DataList.add(map);
			}
			
			br.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return tofEnsb2DataList;
	}
	
	private boolean insertTofEnsb2DataList(List<Map<String, String>> tofEnsb2DataList, DatabaseManager dbManager) {
		
		if(tofEnsb2DataList.size() < 1) {
			return false;
		}
		
		for(int i=0 ; i<tofEnsb2DataList.size() ; i++) {
			
			Map<String, String> tofEnsb2Data = tofEnsb2DataList.get(i);
			
			String insertQuery = "INSERT INTO AAMI.TOF_ENSB2_DATA (ISSUED_TM, FCST_TM, STN_CD, WDIR, WSPD, TEMP, PRES) VALUES "+
					 " (TO_DATE('"+tofEnsb2Data.get("issuedTm")+"', 'YYYY-MM-DD HH24:MI:SS'), "+
					 " TO_DATE('"+tofEnsb2Data.get("fcstTm")+"', 'YYYY-MM-DD HH24:MI:SS'), "+
					 " '"+tofEnsb2Data.get("stnCd")+"', "+
					 " '"+tofEnsb2Data.get("wdir")+"', "+
					 " '"+tofEnsb2Data.get("wspd")+"', "+
					 " '"+tofEnsb2Data.get("temp")+"', "+
					 " '"+tofEnsb2Data.get("pres")+"')";
			
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
			
			//tof_ensb2_2025081109_RKJB.csv
			
			String stnCd = file.getName().split("\\.")[0].split("_")[3];
			String issuedTm = file.getName().split("\\.")[0].split("_")[2];
			
			List<Map<String, String>> tofEnsb2DataList = this.parseTofEnsb2File(file, stnCd, issuedTm);
			
			this.insertTofEnsb2DataList(tofEnsb2DataList, dbManager);
			
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