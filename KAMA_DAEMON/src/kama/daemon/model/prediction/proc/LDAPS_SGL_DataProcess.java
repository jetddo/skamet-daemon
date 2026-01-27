package kama.daemon.model.prediction.proc;

import java.io.File;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Date;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.common.util.Log;
import kama.daemon.model.prediction.adopt.LDAPS_SGL.loader.EquivPotentialTempLoader;
import kama.daemon.model.prediction.adopt.LDAPS_SGL.loader.FogGuidanceLoader;
import kama.daemon.model.prediction.adopt.LDAPS_SGL.loader.FogHumidityLoader;
import kama.daemon.model.prediction.adopt.LDAPS_SGL.loader.UFogLoader;

import org.apache.commons.lang3.time.DateUtils;

/**
 * @author chlee
 * Created on 2017-01-05.
 */
public class LDAPS_SGL_DataProcess extends DataProcessor
{
    //
    // 포함하고 있는 데이터 타입들
    // 1. 안개-습도 예상도
    // 2. 안개-시정
    // 3. 구름변수: 안개가이던스
    // 4. 상당온위
    //
    private static final String DATAFILE_PREFIX = "ldaps_sgl";
    private static final String FILE_TYPE_UFOG = "ldps_l1p5_ufog.*";
    private static final String FILE_TYPE_FOG_HUMIDITY = "ldps_l1p5_fk4s.*";
    private static final String FILE_TYPE_EQUIV_PTL_TEMP = "ldps_lc06_grph.*";
    private static final String FILE_TYPE_FOG_GUIDANCE = "lfog_visb.*";
    private static final int DB_COLUMN_COUNT = 11;
    private static final int FILE_DATE_INDEX_POS = 2; // ldps_l1p5_fk4s.2016110606_000-000
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2 }; // STN_ID, PREDCT_TIME, MDL_PROD_TIME
    private final int INSERT_QUERY = 1;
    private final int TRUNCATE_QUERY = 2;
    private final int DELETE_QUERY = 3;
    private final int MERGE_QUERY_TEMPLATE = 4;

    public LDAPS_SGL_DataProcess(DaemonSettings settings)
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

        // ldps_l1p5_fk4s.2016110606_000-000
        // ldps_l1p5_ufog.2016110606_000

        int[] stn_ids = { 92, 110, 113, 151, 163, 167, 153, 182 };
        double[] lat_values = { 38.06, 37.56, 37.47, 35.59, 35, 34.85, 35.17, 33.51 };
        double[] lon_values = { 128.67, 126.8, 126.44, 129.34, 126.38, 127.61, 128.93, 126.5 };
        
        if (file.getName().matches(FILE_TYPE_UFOG))
        {
        	UFogLoader uFogLoader = new UFogLoader(file.getAbsolutePath());
        	for (int i = 0; i < stn_ids.length; i++)
            {
        		processUFogData(dbManager, file, uFogLoader, processorInfo, stn_ids[i], lat_values[i], lon_values[i]);
            }
        }
        else if (file.getName().matches(FILE_TYPE_FOG_HUMIDITY))
        {
        	FogHumidityLoader fogHumidityLoader = new FogHumidityLoader(file.getAbsolutePath(), 37);
        	for (int i = 0; i < stn_ids.length; i++)
            {
        		processFogHumidityData(dbManager, file, fogHumidityLoader, processorInfo, stn_ids[i], lat_values[i], lon_values[i]);
            }
            //fogHumidityLoader = new FogHumidityLoader(file.getAbsolutePath(), 1);
        }
        else if (file.getName().matches(FILE_TYPE_EQUIV_PTL_TEMP))
        {
        	EquivPotentialTempLoader equivPotentialTempLoader = new EquivPotentialTempLoader(file.getAbsolutePath());
        	for (int i = 0; i < stn_ids.length; i++)
            {
        		processEquivPotentialTempData(dbManager, file, equivPotentialTempLoader, processorInfo, stn_ids[i], lat_values[i], lon_values[i]);
            }
        }
        else if (file.getName().matches(FILE_TYPE_FOG_GUIDANCE))
        {
        	FogGuidanceLoader fogGuidanceLoader = new FogGuidanceLoader(file.getAbsolutePath());
        	for (int i = 0; i < stn_ids.length; i++)
            {
        		processFogGuidanceData(dbManager, file, fogGuidanceLoader, processorInfo, stn_ids[i], lat_values[i], lon_values[i]);
            }
        }
        else
        {
            throw new DaemonException("Unsupported file type detected.");
        }        
    }

    //<editor-fold desc="파일별 데이터 처리 함수들">
    private void processFogGuidanceData(DatabaseManager dbManager, File file, FogGuidanceLoader fogGuidanceLoader, ProcessorInfo processorInfo, int stationID, double lat, double lon) throws Exception
    {   

        String query = "MERGE INTO %%WORKSPACE%%.NMDL_LDAPS_SGL d USING (SELECT ''{0}'' STN_ID, TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') PREDCT_TIME, TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'') MDL_PROD_TIME FROM dual) s ON (d.STN_ID = s.STN_ID AND d.PREDCT_TIME = s.PREDCT_TIME AND d.MDL_PROD_TIME = s.MDL_PROD_TIME) WHEN MATCHED THEN UPDATE SET d.CLOUD_VAR_VSBL = ''{3}'' WHEN NOT MATCHED THEN INSERT(STN_ID, PREDCT_TIME, MDL_PROD_TIME, CLOUD_VAR_VSBL) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'')";
        double value = fogGuidanceLoader.getVisbValue(lat, lon);
        int hours = 0;

        hours = Integer.parseInt(file.getName().split("_")[2]);
        int index = 0;
        Object[] bindArray = new Object[4];

        // CLOUD_VAR_VSBL
        bindArray[index++] = convertToDBText(stationID);
        bindArray[index++] = convertToDBText(DateUtils.addHours(processorInfo.FileDateFromNameKST, hours));
        bindArray[index++] = convertToDBText(processorInfo.FileDateFromNameUTC); // UTC
        bindArray[index++] = convertToDBText(value);

        query = MessageFormat.format(makeQuery(query), bindArray);
        dbManager.executeUpdate(query);
    }

    private void processUFogData(DatabaseManager dbManager, File file, UFogLoader uFogLoader, ProcessorInfo processorInfo, int stationID, double lat, double lon) throws Exception
    {
        
        double[] values;
        int hours = 0;
        
        hours = Integer.parseInt(file.getName().split("_")[3]);

        values = uFogLoader.getVisGValue(lat, lon);

        for (int i = 0; i < values.length; i++)
        {
            String query = "MERGE INTO %%WORKSPACE%%.NMDL_LDAPS_SGL d USING (SELECT ''{0}'' STN_ID, TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') PREDCT_TIME, TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'') MDL_PROD_TIME FROM dual) s ON (d.STN_ID = s.STN_ID AND d.PREDCT_TIME = s.PREDCT_TIME AND d.MDL_PROD_TIME = s.MDL_PROD_TIME) WHEN MATCHED THEN UPDATE SET d.FOG_VSBL = ''{3}'' WHEN NOT MATCHED THEN INSERT(STN_ID, PREDCT_TIME, MDL_PROD_TIME, FOG_VSBL) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'')";

            int index = 0;
            Object[] bindArray = new Object[4];

            // FOG_VSBL
            bindArray[index++] = convertToDBText(stationID);
            bindArray[index++] = convertToDBText(DateUtils.addHours(processorInfo.FileDateFromNameKST, hours + i));
            bindArray[index++] = convertToDBText(processorInfo.FileDateFromNameUTC); // UTC
            bindArray[index++] = convertToDBText(values[i]);

            query = MessageFormat.format(makeQuery(query), bindArray);
            dbManager.executeUpdate(query);
        }
    }

    private void processFogHumidityData(DatabaseManager dbManager, File file, FogHumidityLoader fogHumidityLoader, ProcessorInfo processorInfo, int stationID, double lat, double lon) throws Exception
    {
        
        double[] values;
        Object[] bindArray;
        int index = 0;
        int hours = 0;

        hours = Integer.parseInt(file.getName().split("-")[1]);

        values = fogHumidityLoader.getValue(lat, lon);

        for (int i = 0; i < values.length; i++)
        {
            String query = "MERGE INTO %%WORKSPACE%%.NMDL_LDAPS_SGL d USING (SELECT ''{0}'' STN_ID, TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') PREDCT_TIME, TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'') MDL_PROD_TIME FROM dual) s ON (d.STN_ID = s.STN_ID AND d.PREDCT_TIME = s.PREDCT_TIME AND d.MDL_PROD_TIME = s.MDL_PROD_TIME) WHEN MATCHED THEN UPDATE SET d.HUMID = ''{3}'' WHEN NOT MATCHED THEN INSERT(STN_ID, PREDCT_TIME, MDL_PROD_TIME, HUMID) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'')";

            bindArray = new Object[4];
            index = 0;

            // HUMID
            bindArray[index++] = convertToDBText(stationID);
            bindArray[index++] = convertToDBText(DateUtils.addHours(processorInfo.FileDateFromNameKST, hours + i));
            bindArray[index++] = convertToDBText(processorInfo.FileDateFromNameUTC); // UTC
            bindArray[index++] = convertToDBText(values[i]);

            query = MessageFormat.format(makeQuery(query), bindArray);
            Log.print("INFO : " + query);
            dbManager.executeUpdate(query);
        }
    }

    private void processEquivPotentialTempData(DatabaseManager dbManager, File file, EquivPotentialTempLoader equivPotentialTempLoader, ProcessorInfo processorInfo, int stationID, double lat, double lon) throws Exception
    {
        
        double[] values;
        Object[] bindArray;
        int index = 0;
        int hours = 0;

        hours = Integer.parseInt(file.getName().split("_")[3]);

        String query = "MERGE INTO %%WORKSPACE%%.NMDL_LDAPS_SGL d USING (SELECT ''{0}'' STN_ID, TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') PREDCT_TIME, TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'') MDL_PROD_TIME FROM dual) s ON (d.STN_ID = s.STN_ID AND d.PREDCT_TIME = s.PREDCT_TIME AND d.MDL_PROD_TIME = s.MDL_PROD_TIME) WHEN MATCHED THEN UPDATE SET d.EQV_POTL_TEMP_700 = ''{3}'', d.EQV_POTL_TEMP_850 = ''{4}'', d.EQV_POTL_TEMP_925 = ''{5}'' WHEN NOT MATCHED THEN INSERT(STN_ID, PREDCT_TIME, MDL_PROD_TIME, EQV_POTL_TEMP_700, EQV_POTL_TEMP_850, EQV_POTL_TEMP_925) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'', ''{4}'', ''{5}'')";

        values = new double[3];
        
        values[0] = equivPotentialTempLoader.getEq700Value(lat, lon);
        values[1] = equivPotentialTempLoader.getEq850Value(lat, lon);
        values[2] = equivPotentialTempLoader.getEq925Value(lat, lon);

        bindArray = new Object[6];
        index = 0;

        bindArray[index++] = convertToDBText(stationID);
        bindArray[index++] = convertToDBText(DateUtils.addHours(processorInfo.FileDateFromNameKST, hours));
        bindArray[index++] = convertToDBText(processorInfo.FileDateFromNameUTC); // UTC
        bindArray[index++] = convertToDBText(values[0]);
        bindArray[index++] = convertToDBText(values[1]);
        bindArray[index++] = convertToDBText(values[2]);

        query = MessageFormat.format(makeQuery(query), bindArray);
        dbManager.executeUpdate(query);
    }
    //</editor-fold>

    /**
     * 데이터 파일 종류별 날짜 파싱 함수
     * @param file 데이터 파일
     * @return 날짜값
     * @throws ParseException
     */
    @Override
    protected Date parseDateFromFileName(File file) throws ParseException
    {
        // Create customized date-parsing method if necessary

        String dateString;

        if (file.getName().matches(FILE_TYPE_UFOG) ||
                file.getName().matches(FILE_TYPE_FOG_HUMIDITY) ||
                file.getName().matches(FILE_TYPE_EQUIV_PTL_TEMP) ||
                file.getName().matches(FILE_TYPE_FOG_GUIDANCE))
        {
            dateString = file.getName().split("\\.")[1];
            dateString = dateString.split("_")[0];
        }
        else
        {
            // 해당사항 없으면 super method call
            return super.parseDateFromFileName(file);
        }

        return DateFormatter.parseDate(dateString, "yyyyMMddHH");
    }

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.NMDL_LDAPS_SGL WHERE STN_ID = ''{0}'' AND PREDCT_TIME = TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') AND MDL_PROD_TIME = TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'')");
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