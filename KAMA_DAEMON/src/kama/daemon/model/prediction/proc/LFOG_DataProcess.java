package kama.daemon.model.prediction.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.common.util.Log;
import kama.daemon.model.prediction.adopt.FOG_LR.FogInfoLoader;
import kama.daemon.model.prediction.adopt.FOG_LR.HeightTempLoader;

import java.io.*;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

/**
 * @author chlee
 * Created on 2017-01-25.
 */
public class LFOG_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "lfog";
    private static final String FILE_TYPE_LFOG_VIS = "lfog_vis_[0-9]{12}.asc"; // lfog_visb.2016110512_000
    private static final String FILE_TYPE_LFOG_OUT_BIN = "lfog_out.[0-9]{4}.[0-9]{10}.bin"; // lfog_out.0090.2017011900.bin
    private static final int DB_COLUMN_COUNT = 9;
    private static final int FILE_DATE_INDEX_POS = 2; // lfog_vis_201611051200.asc
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2, 3 }; // STN_ID, PREDCT_TIME, MDL_PROD_TIME, FLOOR
    private final int INSERT_QUERY = 1;
    private final int TRUNCATE_QUERY = 2;
    private final int DELETE_QUERY = 3;

    public LFOG_DataProcess(DaemonSettings settings)
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

        String fileName = file.getName();

        if (fileName.matches(FILE_TYPE_LFOG_VIS))
        {
            // lfog_vis_201611051200.asc
            processDataInternal_fogInfo(dbManager, file, processorInfo);
        }
        else if(fileName.matches(FILE_TYPE_LFOG_OUT_BIN))
        {
            // lfog_out.0090.2017011900.bin
            int stationID;

            stationID = Integer.parseInt(fileName.split("\\.")[1]);
            processDataInternal_heightTemp(dbManager, file, processorInfo, stationID);
        }

        // Record insert example:
        // Remove previous duplicate data to update values
        // query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
        // dbManager.executeUpdate(query);

        // Insert to table
        // query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
        // dbManager.executeUpdate(query);
    }

    private void processDataInternal_fogInfo(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        FogInfoLoader loader;

        loader = new FogInfoLoader(file, processorInfo.FileDateFromNameUTC, processorInfo.FileDateFromNameKST);

        List<FogInfoLoader.FogInfo> fogInfoList = loader.retrieveFogInfoList();

        for (FogInfoLoader.FogInfo info : fogInfoList)
        {
            String queryTemplate = "MERGE INTO %%WORKSPACE%%.NMDL_LFOG d USING (SELECT ''{0}'' STN_ID, TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') PREDCT_TIME, TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'') MDL_PROD_TIME, ''{3}'' FLOOR FROM dual) s ON (d.STN_ID = s.STN_ID AND d.PREDCT_TIME = s.PREDCT_TIME AND d.MDL_PROD_TIME = s.MDL_PROD_TIME AND d.FLOOR = s.FLOOR) WHEN MATCHED THEN UPDATE SET d.LFOG_VSBL = ''{4}'' WHEN NOT MATCHED THEN INSERT(STN_ID, PREDCT_TIME, MDL_PROD_TIME, FLOOR, LFOG_VSBL) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'', ''{4}'')";

            int index = 0;
            Object[] bindArray = new Object[5];
            String query;

            bindArray[index++] = convertToDBText(info.StationID);
            bindArray[index++] = convertToDBText(info.PredictTime);
            bindArray[index++] = convertToDBText(info.ModelProducedTime);
            bindArray[index++] = convertToDBText(0);
            bindArray[index++] = convertToDBText(info.Visibility);

            // merge query (insert)
            query = MessageFormat.format(makeQuery(queryTemplate), bindArray);
            //Log.print("INFO : query " + query);
            dbManager.executeUpdate(query);
        }
    }

    private void processDataInternal_heightTemp(DatabaseManager dbManager, File file, ProcessorInfo processorInfo, int stationID) throws Exception
    {
        HeightTempLoader loader;

        loader = new HeightTempLoader(file, processorInfo.FileDateFromNameUTC, processorInfo.FileDateFromNameKST, stationID);

        List<HeightTempLoader.HeightTempInfo> htInfoList = loader.retrieveData();

        for (HeightTempLoader.HeightTempInfo info : htInfoList)
        {
            String queryTemplate = "MERGE INTO %%WORKSPACE%%.NMDL_LFOG d USING (SELECT ''{0}'' STN_ID, TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') PREDCT_TIME, TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'') MDL_PROD_TIME, ''{3}'' FLOOR FROM dual) s ON (d.STN_ID = s.STN_ID AND d.PREDCT_TIME = s.PREDCT_TIME AND d.MDL_PROD_TIME = s.MDL_PROD_TIME AND d.FLOOR = s.FLOOR) WHEN MATCHED THEN UPDATE SET d.TEMP = ''{4}'', d.DEW_PT = ''{5}'', d.HUMID = ''{6}'', d.PRES = ''{7}'' WHEN NOT MATCHED THEN INSERT(STN_ID, PREDCT_TIME, MDL_PROD_TIME, FLOOR, TEMP, DEW_PT, HUMID, PRES) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'')";

            int index = 0;
            Object[] bindArray = new Object[8];
            String query;

            bindArray[index++] = convertToDBText(info.station_id);
            bindArray[index++] = convertToDBText(info.predicted_time);
            bindArray[index++] = convertToDBText(info.model_produced_time);
            bindArray[index++] = convertToDBText(info.floor);
            bindArray[index++] = convertToDBText(info.temperature);
            bindArray[index++] = convertToDBText(info.dew_point);
            bindArray[index++] = convertToDBText(info.rel_humidity);
            bindArray[index++] = convertToDBText(info.pressure);

            // merge query (insert)
            query = MessageFormat.format(makeQuery(queryTemplate), bindArray);
            dbManager.executeUpdate(query);
        }
    }

    // You can remove this method if unnecessary
    @Override
    protected Date parseDateFromFileName(File file) throws ParseException
    {
        // Create customized date-parsing method if necessary
        //throw new sun.reflect.generics.reflectiveObjects.NotImplementedException();
        //return parseDateFromFileName(file, "yyyyMMddHHmm");
        String dateString;
        String dateFormat;
        String fileName;

        fileName = file.getName();

        if (fileName.matches(FILE_TYPE_LFOG_VIS))
        {
            // lfog_vis_201611051200.asc
            dateString = fileName.split("\\.")[0];
            dateString = dateString.split("_")[2];
            dateFormat = "yyyyMMddHHmm";
        }
        else if(fileName.matches(FILE_TYPE_LFOG_OUT_BIN))
        {
            // lfog_out.0090.2017011900.bin
            dateString = fileName.split("\\.")[2];
            dateFormat = "yyyyMMddHH";
        }
        else
        {
            // 해당사항 없으면 super method call
            return super.parseDateFromFileName(file);
        }

        return DateFormatter.parseDate(dateString, dateFormat);
    }

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.NMDL_LFOG(STN_ID, PREDCT_TIME, MDL_PROD_TIME, FLOOR, LFOG_VSBL, TEMP, DEW_PT, HUMID, PRES) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'')");
        defineQueryFormat(TRUNCATE_QUERY, "DELETE FROM %%WORKSPACE%%.NMDL_LFOG");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.NMDL_LFOG WHERE STN_ID = ''{0}'' AND PREDCT_TIME = TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') AND MDL_PROD_TIME = TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'') AND FLOOR = ''{3}''");
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
