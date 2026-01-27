package kama.daemon.model.observation.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;

/**
 * @author chlee
 * Created on 2016-12-09.
 */
public class SEAVIS_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "seavis";
    private static final int DB_COLUMN_COUNT = 17;
    private static final int FILE_DATE_INDEX_POS = 2; // hiway_min_201604201609
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1 }; // ID, TM
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;

    String insertQuery = "INSERT INTO AAMI.SEAVIS_DATA (STN_ID, TM, VIS1, VIS2) "+
			 "VALUES (''{0}'', TO_DATE(''{1}'', ''YYYYMMDDHH24MI''), ''{2}'', ''{3}'')";
    
    public SEAVIS_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
        this.insertHistory = false;
    }
    
    private void parseSeaVisFile(File file, String tmStr, String stnId, DatabaseManager dbManager) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		
		try {
			
			BufferedReader br = new BufferedReader(new FileReader(file.getAbsoluteFile()));
			
			String line = null;
			
			for (int i=0 ; (line = br.readLine()) != null; i++) {
				
				Calendar cal = new GregorianCalendar();
				
				String[] tokens = line.split(",");
				
				String vis1 = tokens[5];
				String vis2 = tokens[12];
				
				String query = MessageFormat.format(insertQuery, new Object[]{        				
						stnId,
						tmStr,
						vis1,
						vis2
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
    @SuppressWarnings("Duplicates")
    @Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
       
		try {
			
			//SEAVIS_202508190000_44001.txt
			
			String tmStr = file.getName().split("\\.")[0].split("_")[1];			
			String stnId = file.getName().split("\\.")[0].split("_")[2];
			
			this.parseSeaVisFile(file, tmStr, stnId, dbManager);
			
		} catch (Exception e) {
			e.printStackTrace();
			dbManager.rollback();
		}
    }

    //<editor-fold desc="Auto-generated getters (No need to modify)">
    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
    //</editor-fold>

	@Override
	protected void defineQueries() {
		// TODO Auto-generated method stub
		
	}
}