package kama.daemon.model.prediction.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.model.prediction.adopt.UKMO.AirportData;
import kama.daemon.model.prediction.adopt.UKMO.USSTLoader;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * @author chlee
 * Created on 2016-12-29.
 */
public class UKMO_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "ukmo";
    private static final int DB_COLUMN_COUNT = 4;
    private static final int FILE_DATE_INDEX_POS = 1; // usst_raw.2016101800.gdat
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2 }; // STN_ID, PREDCT_TIME, MDL_PROD_TIME
    private final int INSERT_QUERY = 1;
    private final int TRUNCATE_QUERY = 2;
    private final int DELETE_QUERY = 3;

    public UKMO_DataProcess(DaemonSettings settings)
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
        // Parameter Info
        // DatabaseManager dbManager: databaseManager
        // File file: A resource file to process

        //
        // Your code begins here
        //
        USSTLoader loader = new USSTLoader(file.getAbsolutePath());
        float seaTemperature;

        for (int i = 0; i < AirportData.Length; i++)
        {
            String query;
            Object[] bindArray;
            int idx;

            bindArray = new Object[4];
            idx = 0;

            // STN_ID
            bindArray[idx++] = convertToDBText(AirportData.StnIDs[i]);

            // PRECT_TIME: [모델 생산시각 UTC + 24 시간 (예측시간)]
            bindArray[idx++] = convertToDBText(DateUtils.addHours(processorInfo.FileDateFromNameKST, 24));

            // MODE_PROD_TIME
            bindArray[idx++] = convertToDBText(processorInfo.FileDateFromNameUTC);

            // SEA_SURFACE_TEMP: 알짜배기. 해수면 온도
            seaTemperature = loader.GetValue(AirportData.Lats[i], AirportData.Lons[i]);
            bindArray[idx++] = convertToDBText(seaTemperature);

            // Record insert example:
            // Remove previous duplicate data to update values
            query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
            dbManager.executeUpdate(query);

            // Insert to table
            query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
            dbManager.executeUpdate(query);
        }
    }

    /**
     * 파일명으로부터 날짜 정보 파싱
     * @param file
     * @return
     * @throws ParseException
     */
    @Override
    protected Date parseDateFromFileName(File file) throws ParseException
    {
        // Create customized date-parsing method if necessary
        final String format = "yyyyMMddHH";
        Date dtFileDate;
        String[] fileNames;
        String sDateNameWithoutExt;
        int dateFormatLength;

        fileNames = file.getName().split("\\.");
        sDateNameWithoutExt = fileNames[getDateFileIndex()];
        dateFormatLength = format.length();

        try
        {
            dtFileDate = DateFormatter.parseDate(sDateNameWithoutExt, format);
        }
        catch (ParseException ex)
        {
            // 해당사항 없으면 super method call
            return super.parseDateFromFileName(file);
        }

        return dtFileDate;
    }

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.NMDL_UKMO(STN_ID, PREDCT_TIME, MDL_PROD_TIME, SEA_SURFACE_TEMP) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'')");
        defineQueryFormat(TRUNCATE_QUERY, "DELETE FROM %%WORKSPACE%%.NMDL_UKMO");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.NMDL_UKMO WHERE STN_ID = ''{0}'' AND PREDCT_TIME = TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') AND MDL_PROD_TIME = TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'')");
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