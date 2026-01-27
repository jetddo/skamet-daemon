package kama.daemon.model.observation.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.model.observation.adopt.AIREP.AIREP_Data;

import java.io.*;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * @author chlee
 * Created on 2017-02-02.
 */
public class AIREP_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "airep";
    private static final int DB_COLUMN_COUNT = 18;
    private static final int FILE_DATE_INDEX_POS = 2; // AIREP_RKSI_20170126135838
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1 }; // TM, STN_CD
    private final int INSERT_QUERY = 1;
    private final int TRUNCATE_QUERY = 2;
    private final int DELETE_QUERY = 3;

    public AIREP_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }

    @Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        //
        // Your code begins here
        //

        AIREP_Data data = AIREP_Data.loadAIREPData(file);
        Object[] bindArray = new Object[DB_COLUMN_COUNT];
        int index = 0;
        String query;

        // object 입력
        bindArray[index++] = convertToDBText(data.InputTime);
        bindArray[index++] = convertToDBText(data.StationCode);
        bindArray[index++] = convertToDBText(data.InputSystemTime);
        bindArray[index++] = convertToDBText(data.InputName);
        bindArray[index++] = convertToDBText(data.InputIP);
        bindArray[index++] = convertToDBText(data.AircraftType);
        bindArray[index++] = convertToDBText(data.Latitude);
        bindArray[index++] = convertToDBText(data.Longitude);
        bindArray[index++] = convertToDBText(data.ObservedTime);
        bindArray[index++] = convertToDBText(data.FlightLevel);
        bindArray[index++] = convertToDBText(data.Temperature);
        bindArray[index++] = convertToDBText(data.WindDirection);
        bindArray[index++] = convertToDBText(data.WindSpeed);
        bindArray[index++] = convertToDBText(data.Turbulance);
        bindArray[index++] = convertToDBText(data.ACFT_ICING_INTST);
        bindArray[index++] = convertToDBText(data.Humidity);
        bindArray[index++] = convertToDBText(data.RMK);
        bindArray[index++] = convertToDBText(data.MessageText);

        // Remove previous duplicate data to update values
        query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
        dbManager.executeUpdate(query);

        // Insert to table
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
        dbManager.executeUpdate(query);
    }

	// You can remove this method if unnecessary
    /*@Override
    protected Date parseDateFromFileName(File file) throws ParseException
    {
		return super.parseDateFromFileName(file, "yyyyMMddHHmmss");
    }*/

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.AIREP(TM, STN_CD, INP_TM, INP_NM, INP_IP, ACFT_TYPE, LAT, LNGT, OBS_TM, FLIGHT_LEVEL, TMP, WD, WSPD, TURB, ACFT_ICING_INTST, HM, RMK, MSG_TEXT) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', TO_DATE(''{8}'', ''YYYY-MM-DD HH24:mi:ss''), ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'', ''{16}'', ''{17}'')");
        defineQueryFormat(TRUNCATE_QUERY, "DELETE FROM %%WORKSPACE%%.AIREP");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.AIREP WHERE TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND STN_CD = ''{1}''");
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