package kama.daemon.model.observation.proc;

import java.io.File;
import java.text.SimpleDateFormat;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;

/**
 * @author chlee
 * Created on 2016-12-09.
 */
public class LGT_EXT_WWLLN_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "lgt_ext_wwlln";
    private static final int DB_COLUMN_COUNT = 17;
    private static final int FILE_DATE_INDEX_POS = 2; // hiway_min_201604201609
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1 }; // ID, TM
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;

    public LGT_EXT_WWLLN_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
        this.insertHistory = false;
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