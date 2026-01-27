package kama.daemon.model.observation.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;

/**
 * @author chlee
 * Created on 2016-11-21.
 */
public class LGT_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "lgt";
    private static final int DB_COLUMN_COUNT = 6;
    private static final int FILE_DATE_INDEX_POS = 2; // LGT_KMA_201609010000
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2 }; // TM, LAT, LON
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;
    private final int INSERT_QUERY_OLD_1 = 4;
    private final int INSERT_QUERY_OLD_2 = 5;
    private final int DELETE_QUERY_OLD = 6;

    public LGT_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }

    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.KMA_LGT(TM, LAT, LON, ST, NS, T, TM_IN) VALUES  (TO_DATE(''{0}'', ''YYYYMMDDHH24miss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', CURRENT_TIMESTAMP)");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.KMA_LGT WHERE TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND LAT = ''{1}'' AND LON = ''{2}''");

        // ******* 기존 AAMI 버전 3D 파싱용 *******
        defineQueryFormat(INSERT_QUERY_OLD_1, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.LGT(TM1, TM2, LAT, LON, ST, HT, NRS, T, TIME_S) VALUES  (''{0}'', ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', TO_TIMESTAMP(''{8}'',''YYYY-MM-DD HH24:mi:ss.ff''))");
        defineQueryFormat(INSERT_QUERY_OLD_2, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.LGT_B(TM1, TM2, LAT, LON, ST, HT, NRS, T, TIME_S) VALUES  (''{0}'', ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', TO_TIMESTAMP(''{8}'',''YYYY-MM-DD HH24:mi:ss.ff''))");
        defineQueryFormat(DELETE_QUERY_OLD, "DELETE %%WORKSPACE_OLD_AAMI%%.LGT WHERE TIME_S <= TO_TIMESTAMP(SYSDATE)-1"); // 2017/04/05 하루 전 데이터까지 keep 하도록 수정 (기존: 이틀)
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
            // AAMI.LGT 테이블 내 데이터 삭제 (기존 버전)
            query = retrieveQueryFormat(DELETE_QUERY_OLD);
            dbManager.executeUpdate(query);

            for (String line; (line = br.readLine()) != null; )
            {
                String[] rawData;
                int index;

                rawData = line.split(" +");
         
                // Parse date from raw data file
                try
                {
                    String sDateTime;
                    sDateTime = MessageFormat.format("{0} {1}", rawData[1], rawData[2].split("\\.")[0]);
                    
                    bindArray[0] = DateFormatter.changeFormat(sDateTime, "yy-MM-dd HH:mm:ss", "yyyyMMddHHmmss");
                    
                } catch (ParseException pe)
                {
                    throw new DaemonException("Error : unable to parse date");
                }

                // Add value according to the raw data columns
                bindArray[1] = rawData[6]; // LAT
                bindArray[2] = rawData[5]; // LON
                bindArray[3] = rawData[8]; // ST
                bindArray[4] = rawData[11]; // NS
                bindArray[5] = "1".equals(rawData[10]) ? "G" : "C"; // T

                // Remove previous duplicate data to update values
                query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
                dbManager.executeUpdate(query);
             
                // LGT 테이블 입력
                query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
                dbManager.executeUpdate(query);
       
                // 기존 AAMI 데이터 입력
                processOldVersionByLine(dbManager, line);
            }
        }
    }

    //region AAMI 파싱 예전 버전
    private void processOldVersionByLine(DatabaseManager dbManager, String line)
    {
    	String[] rawData = line.replaceAll("\\s+", "#").split("#");
        Object[] bindArray = new Object[9];
        rawData = line.split(" +");
 
        Date dt;
        // Parse date from raw data file
        try
        {
            String sDateTime;
            sDateTime = MessageFormat.format("{0} {1}", rawData[1], rawData[2].split("\\.")[0]);
            
            dt = new SimpleDateFormat("yy-MM-dd HH:mm:ss").parse(sDateTime);
            
            bindArray[0] = new SimpleDateFormat("MM/dd/yy").format(dt);
            bindArray[1] = new SimpleDateFormat("HH:mm:ss.SSS").format(dt);
             
        } catch (ParseException pe)
        {
            throw new DaemonException("Error : unable to parse date");
        }
        
     
        bindArray[2] = rawData[6]; // LAT
        bindArray[3] = rawData[5]; // LON
        bindArray[4] = rawData[8]; // ST
        bindArray[5] = rawData[7]; // HT
        bindArray[6] = rawData[11]; // NRS
        bindArray[7] = "1".equals(rawData[10]) ? "G" : "C"; // T
        
        bindArray[8] = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(dt); // TIME_S
    	
        String query;
        
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_1), bindArray);        
        dbManager.executeUpdate(query);

        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_2), bindArray);
        dbManager.executeUpdate(query);
    }
    //endregion

    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
}