package kama.daemon.model.prediction.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.MessageFormat;
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
public class RDPS_NE36_DataProcess extends DataProcessor
{
	private static final String DATAFILE_PREFIX = "rdps_ne36";
    private static final int DB_COLUMN_COUNT = 24;
    private static final int FILE_DATE_INDEX_POS = 32; // ieodo_20190904.dat (method overridden)
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0 }; // TM
    private final int INSERT_QUERY = 1;
        
    String insertQuery = "INSERT INTO AAMI.RDPS_NE36_DATA (ISSUED_TM, FCST_TM, STN_ID, RH, U, V, T, RH2, T2, U10, V10, SST, PRES) "+
			 "VALUES (TO_DATE(''{0}'', ''YYYYMMDDHH24''), TO_DATE(''{1}'', ''YYYYMMDDHH24''), ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'')";
  	
	public RDPS_NE36_DataProcess(DaemonSettings settings) {
		super(settings, DATAFILE_PREFIX);
		// TODO Auto-generated constructor stub
	}
	
	private void parseRdpsNe36File(File file, String issuedTm, String pres, String stnId, DatabaseManager dbManager) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		
		try {
			
			BufferedReader br = new BufferedReader(new FileReader(file.getAbsoluteFile()));
			
			String line = null;
			
			for (int i=0 ; (line = br.readLine()) != null; i++) {
				
				if(i == 0) {
					continue;
				}
				
				Calendar cal = new GregorianCalendar();
				cal.setTime(sdf.parse(issuedTm));
				
				String[] tokens = line.split(",");
				int fcstHour = Integer.valueOf(tokens[0]);
				cal.add(Calendar.HOUR_OF_DAY, fcstHour);
				
				String rh = tokens[1];
				String u = tokens[2];
				String t = tokens[3];
				String v = tokens[4];
				String rh2 = tokens[5];
				String t2 = tokens[6];
				String u10 = tokens[7];
				String v10 = tokens[8];
				String sst = tokens[9];
				
				String query = MessageFormat.format(insertQuery, new Object[]{        				
					issuedTm,
					sdf.format(cal.getTime()),
					stnId,
					rh,
					u,
					v,
					t,					
					rh2,
					t2,
					u10,
					v10,
					sst,
					pres
    			}).replaceAll("'null'", "null");
                
                dbManager.executeUpdate(query);
			}
			
			br.close();
			
			dbManager.commit();	
	    	
		} catch (Exception e) {
			e.printStackTrace();
			dbManager.rollback();
		}
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
			//ldps_850_airport_092_2024102100.csv
			String issuedTm = file.getName().split("\\.")[1].split("_")[2];						
			String pres = file.getName().split("\\.")[0].split("_")[2];
			String stnId = file.getName().split("\\.")[1].split("_")[1];
			
			this.parseRdpsNe36File(file, issuedTm, pres, stnId, dbManager);
			
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