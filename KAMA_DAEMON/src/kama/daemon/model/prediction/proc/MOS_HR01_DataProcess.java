package kama.daemon.model.prediction.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
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
public class MOS_HR01_DataProcess extends DataProcessor
{
	private static final String DATAFILE_PREFIX = "mos_hr01";
    private static final int DB_COLUMN_COUNT = 24;
    private static final int FILE_DATE_INDEX_POS = 32; // ieodo_20190904.dat (method overridden)
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // TM
    private final int INSERT_QUERY = 1;
    	
	private String[] elemTokenKeys = {
        "TMX", "TMN", "T1H", "REH", "SKY", "VEC", "WSD", "PTY", "POP", "RN1", "SN1", "RN3", "SN3"
    };
	
	public MOS_HR01_DataProcess(DaemonSettings settings) {
		super(settings, DATAFILE_PREFIX);
		// TODO Auto-generated constructor stub
	}
	
	private String getStnCdFromStnId(String stnId) {
    	
    	switch(Integer.valueOf(stnId)) {
    	
    	case 113: return "RKSI";
    	case 114: return "RKNW";
    	case 92: return "RKNY";
    	case 167: return "RKJY";
    	case 110: return "RKSS";
    	case 182: return "RKPC";
    	case 163: return "RKJB";
    	case 151: return "RKPU";
    	case 153: return "RKPK";
    	case 128: return "RKTU";
    	case 142: return "RKTN";
    	case 158: return "RKJJ";
    	case 139: return "RKTH";
    	case 161: return "RKPS";
    	case 118: return "RKNW";
    	case 986: return "RKTL";
    	}
    	
    	return "";
    }
	
	private void clearTokenDatas(Map<String, String[]> tokenDatas) {
		
		for (String key : tokenDatas.keySet()) {
			tokenDatas.put(key, null);
		}
	}
	
	private boolean checkTokenDatas(Map<String, String[]> tokenDatas) {
	
		boolean check = true;

		for (String key : tokenDatas.keySet()) {
			
			if (Arrays.asList(elemTokenKeys).contains(key) && tokenDatas.get(key) == null) {
				check = false;
				break;
			}
		}
		
		for (String key : tokenDatas.keySet()) {
			
			if (tokenDatas.get(key) != null && tokenDatas.get(key).length != 132) {
				check = false;
				break;
			}
		}
		
		return check;
	}
	
	private List<Map<String, String>> parseTokenDatas(Map<String, String[]> tokenDatas, String issuedTm) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		List<Map<String, String>> mosHr01DataList = new ArrayList<Map<String, String>>();
		
		try {
		
			Calendar cal = new GregorianCalendar();
			
			Date issuedDate = sdf.parse(issuedTm.substring(0, 10));
			
			String stnId = tokenDatas.get(this.elemTokenKeys[0])[0].trim();
			
			for(int i=2 ; i<132 ; i++) {
				
				Map<String, String> mosHr01Data = new HashMap<String, String>();
				
				cal.setTime(issuedDate);
				
				mosHr01Data.put("issuedTm", sdf.format(cal.getTime()));				
				
				cal.add(Calendar.HOUR, (i-2)+6);
				
				mosHr01Data.put("fcstTm", sdf.format(cal.getTime()));
				
				mosHr01Data.put("stnId", (Integer.valueOf(stnId)-47000)+"");
				
				for(String key : this.elemTokenKeys) {					
					
					String value = tokenDatas.get(key)[i].trim();
					
					if (value.length() == 0) {
						value = null;
					}
					
					mosHr01Data.put(key, value);
				}
				
				mosHr01DataList.add(mosHr01Data);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}		
		
		return mosHr01DataList;
	}
	
	private void parseHr01File(File file, String issuedTm, DatabaseManager dbManager) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		try {
			
			Map<String, String[]> tokenDatas = new HashMap<String, String[]>();
			
			BufferedReader br = new BufferedReader(new FileReader(file.getAbsoluteFile()));
				
			Calendar cal = new GregorianCalendar();
			
			String line = null;
			
			for (int i=0 ; (line = br.readLine()) != null; i++) {
				
				if(i <= 2) {
					continue;
				}
				
				Map<String, String> map = new HashMap<String, String>();
				
				String[] tokens = line.split(",");
					
				String elem = tokens[1].trim();
				
				if (elem.equals(this.elemTokenKeys[0])) {
					
					tokenDatas.put(elem, tokens);
					
				} else if(elem.equals(this.elemTokenKeys[this.elemTokenKeys.length-1] )) {
					
					tokenDatas.put(elem, tokens);
					
					boolean check = this.checkTokenDatas(tokenDatas);
					
					if(check) {
						
						List<Map<String, String>> mosHr01DataList = this.parseTokenDatas(tokenDatas, issuedTm);
						
						this.insertMosHr01DataList(mosHr01DataList, dbManager);
						
						this.clearTokenDatas(tokenDatas);
					}
					
				} else {
					tokenDatas.put(elem, tokens);
				}
			}
			
			br.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private boolean insertMosHr01DataList(List<Map<String, String>> mosHr01DataList, DatabaseManager dbManager) {
		
		if(mosHr01DataList.size() < 1) {
			return false;
		}
		
		for(int i=0 ; i<mosHr01DataList.size() ; i++) {
			
			Map<String, String> mosHr01Data = mosHr01DataList.get(i);
			
			String stnId = mosHr01Data.get("stnId");
			String stnCd = this.getStnCdFromStnId(stnId);
			
			String insertQuery = "INSERT INTO AAMI.MOS_HR01_DATA (STN_ID, STN_CD, ISSUED_TM, FCST_TM, TMX, TMN, T1H, REH, SKY, WSD, VEC, PTY, POP, RN1, SN1, RN3, SN3) VALUES ("+
					 " '" + stnId + "', "+
					 " '" + stnCd + "', "+
					 " TO_DATE('"+mosHr01Data.get("issuedTm")+"', 'YYYYMMDDHH24'), "+
					 " TO_DATE('"+mosHr01Data.get("fcstTm")+"', 'YYYYMMDDHH24'), "+
					 " " + mosHr01Data.get("TMX") + ", "+
					 " " + mosHr01Data.get("TMN") + ", "+
					 " " + mosHr01Data.get("T1H") + ", " + 
					 " " + mosHr01Data.get("REH") + ", " + 
					 " " + mosHr01Data.get("SKY") + ", " + 
					 " " + mosHr01Data.get("WSD") + ", " + 
					 " " + mosHr01Data.get("VEC") + ", " + 
					 " " + mosHr01Data.get("PTY") + ", " + 
					 " " + mosHr01Data.get("POP") + ", " + 
					 " " + mosHr01Data.get("RN1") + ", " + 
					 " " + mosHr01Data.get("SN1") + ", " + 
					 " " + mosHr01Data.get("RN3") + ", " + 
					 " " + mosHr01Data.get("SN3") + ") ";
			
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
			
			String issuedTm = file.getName().split("\\.")[1];						
			
			this.parseHr01File(file, issuedTm, dbManager);
			
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