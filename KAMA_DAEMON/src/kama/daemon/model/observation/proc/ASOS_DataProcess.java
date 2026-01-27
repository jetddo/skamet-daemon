package kama.daemon.model.observation.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Arrays;

/**
 * @author chlee
 * Created on 2016-12-07.
 */
public class ASOS_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "asos";
    private static final int DB_COLUMN_COUNT = 56;
    private static final int FILE_DATE_INDEX_POS = 1; // SHKO60_201508111305
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1 }; // STN_ID, TM
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;

    public ASOS_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }

    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.ASOS(STN_ID, TM, WD_DEG, WD, WS, WS_UNT, WD_GST, WS_GST, WS_GST_TM, PA, PS, PR, PT, TA, TD, HM, PV, RN, RN_ACC, SD_HR1, SD_HR3, SD_DAY, SD_TOT, WC, WP, WW, CA_TOT, CA_MID, CH_MIN, CT, CT_TOP, CT_MID, CT_LOW, VS, SS_HR1, SI_HR1, TG, ST_GD, TS, TE_005, TE_01, TE_02, TE_03, TE_05, TE_10, TE_15, TE_30, TE_50, WH, RN_INT, RN_DAY, ST_SEA, BF, RN_JUN, IR, IX) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYYMMDDHH24miss''), ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'', ''{16}'', ''{17}'', ''{18}'', ''{19}'', ''{20}'', ''{21}'', ''{22}'', ''{23}'', ''{24}'', ''{25}'', ''{26}'', ''{27}'', ''{28}'', ''{29}'', ''{30}'', ''{31}'', ''{32}'', ''{33}'', ''{34}'', ''{35}'', ''{36}'', ''{37}'', ''{38}'', ''{39}'', ''{40}'', ''{41}'', ''{42}'', ''{43}'', ''{44}'', ''{45}'', ''{46}'', ''{47}'', ''{48}'', ''{49}'', ''{50}'', ''{51}'', ''{52}'', ''{53}'', ''{54}'', ''{55}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.ASOS WHERE STN_ID = ''{0}'' AND TM = TO_DATE(''{1}'', ''YYYYMMDDHH24miss'')");
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
        String query;
        Object[] bindArray;

        query = null;
        bindArray = new Object[DB_COLUMN_COUNT];
        Arrays.fill(bindArray, "");

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            for (String line; (line = br.readLine()) != null; )
            {
                String[] rawData;
                int index;

                rawData = line.split("#");

                // Parse date from raw data file
                try
                {
                    String sDateTime;
                    sDateTime = rawData[1];
                    bindArray[0] = DateFormatter.changeFormat(sDateTime, "yyyyMMddHHmm", "yyyyMMddHHmmss");
                } catch (ParseException pe)
                {
                    throw new DaemonException("Error : unable to parse date");
                }

                // Add value according to the hashmap
                for (int i = 0; i < bindArray.length; i++)
                {
                    bindArray[i] = rawData[i];

                    // 마지막 2개 컬럼에 대해서는 -900보다 작을시 NULL 처리. (ORA-01438 오류 대체용)
                    if (i >= 54)
                    {
                        try
                        {
                            if (Integer.parseInt(rawData[i]) < -900)
                            {
                                bindArray[i] = "NULL";
                            }
                        } catch (NumberFormatException ne)
                        {
                            bindArray[i] = "NULL";
                        }
                    }
                }

                // Remove previous duplicate data to update values
                query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
                query = processNULLFromQuery(query);
                dbManager.executeUpdate(query);

                // Insert data to table
                query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
                query = processNULLFromQuery(query);
                dbManager.executeUpdate(query);
            }
        }
    }

    private String processNULLFromQuery(String query)
    {
        return query.replace("'NULL'", "NULL");
    }

    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
}