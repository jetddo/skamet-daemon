package kama.daemon.model.prediction.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.model.prediction.adopt.WAFC.WAFCLoader;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * @author chlee
 * Created on 2017-01-24.
 */
public class WAFC_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "wafc";
    private static final String WAFC_MAINDATAFILE = "[0-9]{8}_[0-9]{4}f[0-9]{2}.grib2";
    private static final String WAFC_SUB1DATAFILE = "[0-9]{8}_[0-9]{4}f[0-9]{2}.grib2.ncx3";
    private static final String WAFC_SUB2DATAFILE = "[0-9]{8}_[0-9]{4}f[0-9]{2}.grib2.gbx9";
    private static final int DB_COLUMN_COUNT = 4;
    private static final int FILE_DATE_INDEX_POS = 1; // 20161009_0600f36.grib2
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2 }; // MDL_ID, PREDCT_TIME, MODEL_PROD_TIME
    private final int INSERT_QUERY = 1;
    private final int TRUNCATE_QUERY = 2;
    private final int DELETE_QUERY = 3;

    public WAFC_DataProcess(DaemonSettings settings)
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
        // Parameter Info
        // DatabaseManager dbManager: databaseManager
        // File file: A resource file to process

        //
        // Your code begins here
        //

        int[] stnIDs = { 22101, 22102, 22186, 22103, 22188, 22184, 22104, 22105, 22189 };
        float[] lats = { 37.24f, 34.79f, 35.66f, 34.0f, 34.39f, 33.79f, 34.77f, 37.48f, 35.35f };
        final float[] lons = { 126.02f, 125.78f, 125.81f, 127.5f, 128.23f, 126.14f, 128.9f, 129.95f, 129.84f };

        String filename = file.getName();

        if (filename.matches(WAFC_MAINDATAFILE))
        {
            WAFCLoader loader = new WAFCLoader(file.getAbsolutePath());

            for (int i = 0; i < stnIDs.length; i++)
            {
                Object[] bindArray;
                int index = 0;
                int forecastHours;
                String temp;
                String query;

                double tropo_value = loader.getTropoHeight(lats[i], lons[i]);

                temp = file.getName().split(Pattern.quote(".grib"))[0];
                temp = temp.split(Pattern.quote("f"))[1];

                forecastHours = Integer.parseInt(temp);

                bindArray = new Object[DB_COLUMN_COUNT];
                bindArray[index++] = convertToDBText(stnIDs[i]);
                bindArray[index++] = convertToDBText(DateUtils.addHours(processorInfo.FileDateFromNameKST, forecastHours));
                bindArray[index++] = convertToDBText(processorInfo.FileDateFromNameUTC);
                bindArray[index++] = convertToDBText(tropo_value);

                // Insert to table
                query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
                dbManager.executeUpdate(query);
            }
        }
    }

	// You can remove this method if unnecessary
    @Override
    protected Date parseDateFromFileName(File file) throws ParseException
    {
        String dateStr;

        dateStr = file.getName().substring(0, 12);
        dateStr = dateStr.replace("_", "");

        try
        {
            return DateFormatter.parseDate(dateStr, "yyyyMMddHHmm");
        }
        catch (ParseException ex)
        {
            // 해당사항 없으면 super method call
            return super.parseDateFromFileName(file);
        }
    }

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.NMDL_WAFC(STN_ID, PREDCT_TIME, MDL_PROD_TIME, TROPO_HT) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'')");
        defineQueryFormat(TRUNCATE_QUERY, "DELETE FROM %%WORKSPACE%%.NMDL_WAFC");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.NMDL_WAFC WHERE STN_ID = ''{0}'' AND PREDCT_TIME = TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') AND MDL_PROD_TIME = TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'')");
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