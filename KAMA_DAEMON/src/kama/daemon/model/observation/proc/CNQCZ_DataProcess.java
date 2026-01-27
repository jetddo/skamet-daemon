package kama.daemon.model.observation.proc;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.model.observation.adopt.RDR.RDRImgDataProcessor;

import java.io.*;
import java.text.MessageFormat;

/**
 * @author chlee
 * Created on 2017-01-03.
 */
public class CNQCZ_DataProcess extends RDRImgDataProcessor
{
    private static final String DATAFILE_PREFIX = "cnqcz";
    private static final int DB_COLUMN_COUNT = 3;
    private static final int FILE_DATE_INDEX_POS = 3; // RDR_CNQCZ_3D80_201609010000.bin.gz
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 2 }; // DUMMY
    private final int INSERT_QUERY_OLD_1 = 1;
    private final int INSERT_QUERY_OLD_2 = 2;
    private final int DELETE_QUERY_OLD = 4;

    public CNQCZ_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
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
        String query = null;
        Object[] bindArray = new Object[2];

        // RDR 처리 (AMIS; 새로운 DB)
        processRDRImage(dbManager, file, processorInfo);

        // AAMI 테이블 입력
        bindArray[0] = convertToDBText(processorInfo.FileDateFromNameOriginal);
        bindArray[1] = convertToDBText(file.getName());

        // RDR 테이블 삭제
        query = retrieveQueryFormat(DELETE_QUERY_OLD);
        dbManager.executeUpdate(query);

        // RDR 테이블 입력
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_1), bindArray);
        dbManager.executeUpdate(query);

        // RDR_B 테이블 입력
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_2), bindArray);
        dbManager.executeUpdate(query);
    }

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        // ******* 기존 AAMI 버전 3D 파싱용 *******
        defineQueryFormat(INSERT_QUERY_OLD_1, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.RDR(TM, FILENAME) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'')");
        defineQueryFormat(INSERT_QUERY_OLD_2, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.RDR_B(TM, FILENAME) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'')");
        defineQueryFormat(DELETE_QUERY_OLD, "DELETE %%WORKSPACE_OLD_AAMI%%.RDR");
    }
    //</editor-fold>

    //<editor-fold desc="Auto-generated getters (No need to modify)">
    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
    //</editor-fold>
}