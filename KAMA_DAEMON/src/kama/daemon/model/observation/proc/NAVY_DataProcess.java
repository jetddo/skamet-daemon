package kama.daemon.model.observation.proc;

import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.model.observation.adopt.NAVY.NAVYData;
import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;

import java.io.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chlee
 * Created on 2016-12-12.
 */
public class NAVY_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "navy";
    private static final int DB_COLUMN_COUNT = 20;
    private static final int FILE_DATE_INDEX_POS = 1; // NAVY_201609010000_1101
    private static final int[] DB_PRIMARY_KEY_INDEXES = {0, 1}; // TM, STN_ID
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;

    public NAVY_DataProcess(DaemonSettings settings)
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
        NAVYData navyData;
        String query;
        String[] entries;

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            navyData = new NAVYData();

            while ((line = br.readLine()) != null)
            {
                // token: #
                token = line.split("#");

                if (token.length > 6) // minimum token length to parse
                {
                	
                    navyData.RecordTime = DateFormatter.parseDate(token[0], "yyyyMMddHHmm");
                    navyData.StationID = Integer.parseInt(token[1]);
                    navyData.MOR = token[7] == null || "".equals(token[7]) ? -999 : Double.parseDouble(token[7]);
                    navyData.humidity = token[10] == null || "".equals(token[10]) ? -999 : Double.parseDouble(token[10]);
                    navyData.windDirection = token[4] == null || "".equals(token[4]) ? -999 : Double.parseDouble(token[4]);
                    navyData.windSpeed = token[5] == null || "".equals(token[5]) ? -999 : Double.parseDouble(token[5]);
                    navyData.temperature = token[9] == null || "".equals(token[9]) ? -999 : Double.parseDouble(token[9]);
                    
                    String cloudInfo = token[16];
                    
                    if("SKC".equals(cloudInfo)) {
                    	navyData.cloudHeight = 9999;
                    } else {
                    	navyData.cloudHeight = Double.parseDouble(cloudInfo.substring(3));
                    }

                    entries = convertToRecordFormat(navyData);

                    // Record insert example:
                    // Remove previous duplicate data to update values
                    query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), entries, DB_PRIMARY_KEY_INDEXES);
                    dbManager.executeUpdate(query);

                    // Insert to table
                    query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), (Object[]) entries);
                    dbManager.executeUpdate(query);
                }
            }
        }
    }

    private String[] convertToRecordFormat(NAVYData navyData)
    {
        List<String> lstTokens;
        String[] sTokens;

        lstTokens = new ArrayList<String>();

        // 현재 3개의 레코드만 identify 된 상태.
        // 이후 Insert query 수정하여 column 추가 수정하면 됨.
        lstTokens.add(convertToDBText(navyData.RecordTime));
        lstTokens.add(convertToDBText(navyData.StationID));
        lstTokens.add(convertToDBText(navyData.MOR));
        lstTokens.add(convertToDBText(navyData.windDirection));
        lstTokens.add(convertToDBText(navyData.windSpeed));
        lstTokens.add(convertToDBText(navyData.temperature));
        lstTokens.add(convertToDBText(navyData.humidity));
        lstTokens.add(convertToDBText(navyData.cloudHeight));
        
        sTokens = new String[lstTokens.size()];

        return lstTokens.toArray(sTokens);
    }

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        // Insert query 원본 (수정시 참고요망)
        //defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.NAVY(TM, STN_ID, WW, CA_TOT, WD, WS, WS_MAX, VS, WH, TA, HM, PA, PS, RN_HR1, SD_HR1, SD_TOT, ST_WW1, ST_WW2, ST_WW3, ST_WW4) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'', ''{16}'', ''{17}'', ''{18}'', ''{19}'')");
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.NAVY(TM, STN_ID, VS, WD, WS, TA, HM, CH) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.NAVY WHERE TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND STN_ID = ''{1}''");
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