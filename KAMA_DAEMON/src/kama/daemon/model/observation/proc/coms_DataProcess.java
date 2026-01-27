package kama.daemon.model.observation.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;

import java.io.*;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * @author chlee
 * Created on 2017-01-03.
 */
public class coms_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "coms";
    private static final int DB_COLUMN_COUNT = 3;
    private static final int FILE_DATE_INDEX_POS = 5; // coms_mi_le1b_com_k_201609010000.thn.png
    //private static final int[] DB_PRIMARY_KEY_INDEXES = { 3 }; // DUMMY
    private final int INSERT_QUERY_OLD_1 = 1;
    private final int INSERT_QUERY_OLD_2 = 2;
    private final int DELETE_QUERY_OLD = 4;

    public coms_DataProcess(DaemonSettings settings)
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
    @Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws DaemonException
    {
        String query = null;
        Object[] bindArray = new Object[3];
        int index = 0;

        bindArray[index++] = convertToDBText(processorInfo.FileDateFromNameOriginal); // DT
        bindArray[index++] = file.getName(); // FILENAME
        bindArray[index++] = file.getName().split("_")[3]; // ID

        // Record insert example:
        // Remove previous duplicate data to update values
        query = retrieveQueryFormat(DELETE_QUERY_OLD);
        dbManager.executeUpdate(query);

        // Insert to table
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_1), bindArray);
        dbManager.executeUpdate(query);
    }

    /**
     * 파일 이름으로부터 날짜값 추출하는 함수
     * @param file 추출할 파일 (일반적으로, 현재 처리중인 데이터 파일)
     * @return 추출한 날짜값
     * @throws ParseException
     */
    @Override
    protected Date parseDateFromFileName(File file) throws ParseException
    {
        String[] fileNameArray = file.getName().split("_");
        String dateFileTokWithoutExt;

        if (!"color".equals(fileNameArray[5]))
        {
            dateFileTokWithoutExt = fileNameArray[5].split("\\.")[0];
        }
        else
        {
            dateFileTokWithoutExt = fileNameArray[6].split("\\.")[0];
        }

        try
        {
            return DateFormatter.parseDate(dateFileTokWithoutExt, "yyyyMMddHHmm");
        }
        catch (ParseException pe)
        {
            return super.parseDateFromFileName(file);
        }
    }

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        // ******* 기존 AAMI 버전 3D 파싱용 *******
        defineQueryFormat(INSERT_QUERY_OLD_1, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.COMS(DT, FILENAME, ID) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'')");
        defineQueryFormat(INSERT_QUERY_OLD_2, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.COMS_B(DT, FILENAME, ID) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'')");
        defineQueryFormat(DELETE_QUERY_OLD, "DELETE %%WORKSPACE_OLD_AAMI%%.COMS WHERE DT < ADD_MONTHS(SYSDATE, -4)");
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