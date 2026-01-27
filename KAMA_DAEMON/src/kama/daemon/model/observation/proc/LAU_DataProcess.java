package kama.daemon.model.observation.proc;

import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.model.observation.adopt.LAU.LAUData;
import kama.daemon.model.observation.adopt.LAU.LAUReader;
import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author chlee
 * Created on 2016-12-05.
 */
public class LAU_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "lau";
    private static final int DB_COLUMN_COUNT = 28;
    private static final int FILE_DATE_INDEX_POS = 2; // LAU_4010_2016110600000380.bin (method overridden)
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 4 }; // TM, LAU_ID, TM_AWS
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;
    private final int DDL_QUERY = 4;

    public LAU_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }

    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.LAU(TM, LAU_ID, PRT_DT, STN_ID, TM_AWS, RCD_CLSF, RCD_TP_NUM, TA_1MIN_AVG, WD_1MIN_AVG, WS_1MIN_AVG, WD_1MIN_GUST, WS_1MIN_GUST, RN_DAY_05MM, QFE_1MIN_AVG, RN_1MIN_YN, SNOW_10MIN, HM_1MIN_AVG, RN_DAY_01MM, TA_05M, TA_40M, BASE_20CM, BASE_30CM, BASE_50CM, CLA_20CM, CLA_30CM, CLA_50CM, ILM) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD''), ''{1}'', TO_DATE(''{2}'', ''YYYY-MM-DD''), ''{3}'', TO_DATE(''{4}'', ''YYYY-MM-DD HH24:mi:ss''), ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'', ''{16}'', ''{17}'', ''{18}'', ''{19}'', ''{20}'', ''{21}'', ''{22}'', ''{23}'', ''{24}'', ''{25}'', ''{26}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.LAU WHERE TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND LAU_ID = ''{1}'' AND TM_AWS = TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'')");
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
        LAUReader lauReader;

        String query;

        lauReader = new LAUReader();
        query = null;

        Date fileTime;
        Object[] record;
        List<LAUData> lstLAUData;

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            try
            {
                fileTime = parseDateFromFileName(file);
            }
            catch (ParseException pe)
            {
                throw new DaemonException(String.format("%s\n%s", "Error : unable to parse date from file name.", pe.toString()));
            }

            // Retrieve records using the previous source code
            lstLAUData = lauReader.parseFile(file.getAbsolutePath());

            for (LAUData lauData : lstLAUData)
            {
                record = LAUDataToObjectArr(lauData);

                // Check the integrity of each record
                if (record.length != DB_COLUMN_COUNT)
                {
                    throw new DaemonException("Error : unexpected number of columns detected on parsing records.");
                }

                // Remove previous duplicate data to update values
                query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), record, DB_PRIMARY_KEY_INDEXES);
                dbManager.executeUpdate(query);

                // Insert data to table
                query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), record);
                dbManager.executeUpdate(query);
            }
        }
    }

    private Object[] LAUDataToObjectArr(LAUData lauData)
    {
        List<Object> objRecord;

        objRecord = new ArrayList<Object>();

        objRecord.add(String.format("%04d-%02d-%02d",
                lauData.Header.Year, lauData.Header.Month, lauData.Header.Day));
        objRecord.add(String.format("%d", lauData.Header.LAUID));
        objRecord.add(String.format("%04d-%02d-%02d",
                lauData.ProtocolYear, lauData.ProtocolMonth, lauData.ProtocolDay));
        objRecord.add(String.format("%d", lauData.AWSID));
        objRecord.add(String.format("%04d-%02d-%02d %02d:%02d:%02d",
                lauData.AWSYear, lauData.AWSMonth, lauData.AWSDay, lauData.AWSHour, lauData.AWSMonth, lauData.AWSMinute));
        objRecord.add(String.format("%d", lauData.AWSID));
        objRecord.add(String.format("%d", lauData.DataType));
        objRecord.add(String.format("%d", lauData.DataTypeIndex));

        for (int i = 0; i < 11; i++)
        {
            objRecord.add(String.format("%d", lauData.FooterData.get(i)));
        }

        for (int i = 18; i < 22 + 5; i++)
        {
            objRecord.add(String.format("%d", lauData.FooterData.get(i)));
        }

        return objRecord.toArray();
    }

    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
}
