package kama.daemon.model.observation.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;

import java.io.*;
import java.lang.reflect.Array;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

/**
 * @author chlee
 * Created on 2017-01-10.
 * 미군 AWS
 */
public class AWOS_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "awos";
    private static final int DB_COLUMN_COUNT = 51;
    private static final int FILE_DATE_INDEX_POS = 3; // AWOS_MIN_AAF_201701061510.txt
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2, 3 }; // TM, DATA_TYPE, DATA_FMT, STN_ID
    private final int INSERT_QUERY = 1;
    private final int TRUNCATE_QUERY = 2;
    private final int DELETE_QUERY = 3;

    public AWOS_DataProcess(DaemonSettings settings)
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
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        String line;
        String[] token;
        Object[] bindArray;
        int index;
        String query;

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            bindArray = new Object[DB_COLUMN_COUNT];
            index = 0;

            line = br.readLine();

            // token: #
            token = line.split("#");

            for (int i = 0; i < DB_COLUMN_COUNT; i++)
            {
                bindArray[i] = convertToDBText(token[i]);
            }
        }

        // Record insert example:
        // Remove previous duplicate data to update values
        query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
        dbManager.executeUpdate(query);

        // Insert to table
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
        dbManager.executeUpdate(query);
    }

	/*// You can remove this method if unnecessary
    @Override
    protected Date parseDateFromFileName(File file) throws ParseException
    {
        // Create customized date-parsing method if necessary
        //throw new sun.reflect.generics.reflectiveObjects.NotImplementedException();
		return parseDateFromFileName(file, "yyyyMMddHHmm");
    }*/

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.UAF_AWOS_MIN(TM, DATA_TYPE, DATA_FMT, STN_ID, TP, WD, WSPD, GUST_WD, GUST_WSPD, RN_1DD, PRSS, RN_YN, SNOW_10MIN, HM, RN_1DD_2, SPR_L1, SPR_L2, SPR_L3, SPR_L4, SPR_L5, SPR_L6, SPR_BASE, SPR_CLA, SPR_MTPH, SPR_VIS, SI, SS, TS, TG, TE_AVG_1MIN_005, TE_AVG_1MIN_010, TE_AVG_1MIN_020, TE_AVG_1MIN_030, TE_AVG_1MIN_050, TE_AVG_1MIN_100, TE_AVG_1MIN_150, TE_AVG_1MIN_300, TE_AVG_1MIN_500, SPR_S1, SPR_S2, SPR_S3, SPR_S4, SPR_S5, SPR_S6, SPR_S7, SPR_S8, SPR_S9, SPR_S10, VT_STS, SENS_STS, SENS_STS2) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'', ''{16}'', ''{17}'', ''{18}'', ''{19}'', ''{20}'', ''{21}'', ''{22}'', ''{23}'', ''{24}'', ''{25}'', ''{26}'', ''{27}'', ''{28}'', ''{29}'', ''{30}'', ''{31}'', ''{32}'', ''{33}'', ''{34}'', ''{35}'', ''{36}'', ''{37}'', ''{38}'', ''{39}'', ''{40}'', ''{41}'', ''{42}'', ''{43}'', ''{44}'', ''{45}'', ''{46}'', ''{47}'', ''{48}'', ''{49}'', ''{50}'')");
        defineQueryFormat(TRUNCATE_QUERY, "DELETE FROM %%WORKSPACE%%.UAF_AWOS_MIN");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.UAF_AWOS_MIN WHERE TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND DATA_TYPE = ''{1}'' AND DATA_FMT = ''{2}'' AND STN_ID = ''{3}''");
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