package kama.daemon.model.observation.proc;

import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.model.observation.adopt.AMDAR_DataConv;
import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

/**
 * @author chlee
 * Created on 2016-11-28.
 */
public class AMDAR_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "amdar";
    private static final int DB_COLUMN_COUNT = 11;
    private static final int FILE_DATE_INDEX_POS = 1; // AAMDAR_201609010013HL7772_1.txt (method overridden)
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2, 3 }; // TM, LATITUDE, LONGITUDE, ALTITUDE
    private static final int[] DB_PRIMARY_KEY_INDEXES_OLD = { 0 }; // STN_ID
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;
    private final int INSERT_QUERY_OLD_1 = 4;
    private final int INSERT_QUERY_OLD_2 = 5;
    private final int DELETE_QUERY_OLD = 6;

    public AMDAR_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }

    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.AMDAR_KOR(TM, LATITUDE, LONGITUDE, ALTITUDE, FLIGHT_ID, TEMP,WD, WSPD, S_AIRPORT, D_AIRPORT, FLY_STAT) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.AMDAR_KOR WHERE TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND LATITUDE = ROUND(''{1}'') AND LONGITUDE = ROUND(''{2}'') AND ALTITUDE = ROUND(''{3}'')");

        // ******* 기존 AAMI 버전 3D 파싱용 *******
        defineQueryFormat(INSERT_QUERY_OLD_1, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.AMDAR(STN_ID, LAT, LON, HT, TEMP, WD, WS, UTC, FLY, NUM, AIR_S, AIR_E, STN_NUM) VALUES ( ''{0}'', ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', TO_DATE(''{7}'', ''YYYY-MM-DD HH24:mi:ss''),''{8}'',''{9}'',''{10}'',''{11}'',''{12}'')");
        defineQueryFormat(INSERT_QUERY_OLD_2, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.AMDAR_B(STN_ID, LAT, LON, HT, TEMP, WD, WS, UTC, FLY, NUM, AIR_S, AIR_E, STN_NUM) VALUES ( ''{0}'', ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', TO_DATE(''{7}'', ''YYYY-MM-DD HH24:mi:ss''),''{8}'',''{9}'',''{10}'',''{11}'',''{12}'')");
        // 2017.06.30: STN_ID 에 따른 데이터가 안들어오면 작년자료도 계속 남는 문제로 인해 임시로 쿼리 수정
        //defineQueryFormat(DELETE_QUERY_OLD, "DELETE %%WORKSPACE_OLD_AAMI%%.AMDAR WHERE STN_ID=''{0}'' AND UTC <= SYSDATE-1");
        defineQueryFormat(DELETE_QUERY_OLD, "DELETE %%WORKSPACE_OLD_AAMI%%.AMDAR WHERE UTC <= SYSDATE-1");
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
        AMDAR_DataConv dataConverter;
        String query;

        dataConverter = new AMDAR_DataConv();
        query = null;

        Date fileTime;
        List<Object[]> records;

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
            records = dataConverter.retrieveRecords(br, fileTime);

            for (Object[] objRecord : records)
            {
                // Check the integrity of each record
                if (objRecord.length != DB_COLUMN_COUNT)
                {
                    throw new DaemonException("Error : unexpected number of columns detected on parsing records.");
                }

                // Remove previous duplicate data to update values
                query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), objRecord, DB_PRIMARY_KEY_INDEXES);
                dbManager.executeUpdate(query);

                // Insert data to table
                query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), objRecord);
                dbManager.executeUpdate(query);
            }
        }

        processOldVersionByFile(dbManager, file, processorInfo.FileDateFromNameOriginal, dataConverter);
    }

    private void processOldVersionByFile(DatabaseManager dbManager, File file, Date fileTime, AMDAR_DataConv dataConverter) throws IOException
    {
        String query;
        List<Object[]> records;

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            records = dataConverter.retrieveOldRecords(br, fileTime);
        }

        // Remove previous duplicate data to update values
        query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY_OLD), records.get(0), DB_PRIMARY_KEY_INDEXES_OLD);
        dbManager.executeUpdate(query);

        for (Object[] objRecord : records)
        {
            // AMDAR 테이블 입력
            query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_1), objRecord);
            dbManager.executeUpdate(query);

            // AMDAR_B 테이블 입력
            query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_2), objRecord);
            dbManager.executeUpdate(query);
        }
    }

    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }

    @Override
    protected Date parseDateFromFileName(File file) throws ParseException
    {
        Date dtFileDate;
        String[] fileNames;
        String sDateNameWithoutExt;

        fileNames = file.getName().split("_");
        sDateNameWithoutExt = fileNames[getDateFileIndex()].split("\\.")[0].substring(0, 12);

        try
        {
            dtFileDate = DateFormatter.parseDate(sDateNameWithoutExt, "yyyyMMddHHmm");
        }
        catch (ParseException ex)
        {
            // 해당사항 없으면 super method call
            return super.parseDateFromFileName(file);
        }

        return dtFileDate;
    }
}