package kama.daemon.model.observation.proc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.model.observation.adopt.HIWAY.Hiway;

/**
 * @author chlee
 * Created on 2016-12-09.
 */
public class hiway_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "hiway";
    private static final int DB_COLUMN_COUNT = 17;
    private static final int FILE_DATE_INDEX_POS = 2; // hiway_min_201604201609
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1 }; // ID, TM
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;

    public hiway_DataProcess(DaemonSettings settings)
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
    @SuppressWarnings("Duplicates")
    @Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        String line;
        String[] token;
        List<Hiway> lstHiway;
        Hiway hiway;
        String query;

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            lstHiway = new ArrayList<Hiway>();

            // 첫째 줄 버리기
            line = br.readLine();

            while ((line = br.readLine()) != null)
            {
                // token: #
                token = line.split("#");

                hiway = new Hiway();
                hiway.IDCode = token[0];
                hiway.RecordTime = DateFormatter.parseDate(token[1], "yyyyMMddHHmm");
                hiway.Latitude = Math.round(Double.parseDouble(token[2]) * 10000) / 10000;
                hiway.Longitude = Math.round(Double.parseDouble(token[3]) * 10000) / 10000;
                hiway.Height = Double.parseDouble(token[4]);
                hiway.wd_max = Double.parseDouble(token[6]);
                hiway.ws_average = Double.parseDouble(token[7]);
                hiway.ws_max = Double.parseDouble(token[8]);
                hiway.tmp_average = Double.parseDouble(token[9]);
                hiway.tmp_min = Double.parseDouble(token[10]);
                hiway.tmp_max = Double.parseDouble(token[11]);
                hiway.hm_average = Double.parseDouble(token[12]);
                hiway.hm_min = Double.parseDouble(token[13]);
                hiway.hm_max = Double.parseDouble(token[14]);
                hiway.mor_average = Integer.parseInt(token[15]);
                hiway.mor_min = Double.parseDouble(token[16]);
                hiway.mor_max = Double.parseDouble(token[17]);

                lstHiway.add(hiway);
            }

            for (Hiway hw : lstHiway)
            {
                token = convertToRecordFormat(hw);

                // Record insert example:
                // Remove previous duplicate data to update values
                query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), token, DB_PRIMARY_KEY_INDEXES);
                dbManager.executeUpdate(query);

                // Insert to table
                query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), (Object[]) token);
                dbManager.executeUpdate(query);
            }
        }
    }

    private String[] convertToRecordFormat(Hiway hiway)
    {
        List<String> lstTokens;
        String[] tokens;

        lstTokens = new ArrayList<String>();

        lstTokens.add(hiway.IDCode);
        lstTokens.add(convertToDBText(hiway.RecordTime));
        lstTokens.add(convertToDBText(hiway.Longitude));
        lstTokens.add(convertToDBText(hiway.Latitude));
        lstTokens.add(convertToDBText(hiway.Height));
        lstTokens.add(convertToDBText(hiway.wd_max));
        lstTokens.add(convertToDBText(hiway.ws_average));
        lstTokens.add(convertToDBText(hiway.ws_max));
        lstTokens.add(convertToDBText(hiway.tmp_average));
        lstTokens.add(convertToDBText(hiway.tmp_min));
        lstTokens.add(convertToDBText(hiway.tmp_max));
        lstTokens.add(convertToDBText(hiway.hm_average));
        lstTokens.add(convertToDBText(hiway.hm_min));
        lstTokens.add(convertToDBText(hiway.hm_max));
        lstTokens.add(convertToDBText(hiway.mor_average));
        lstTokens.add(convertToDBText(hiway.mor_min));
        lstTokens.add(convertToDBText(hiway.mor_max));

        tokens = new String[lstTokens.size()];

        return lstTokens.toArray(tokens);
    }

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        // ******* 기존 AAMI 버전 3D 파싱용 *******
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.HIWAY_MIN(ID, TM, LON, LAT, HT, WD_MAX, WS_AVG, WS_MAX, TMP_AVG, TMP_MIN, TMP_MAX, HM_AVG, HM_MIN, HM_MAX, MOR_AVG, MOR_MIN, MOR_MAX) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'', ''{16}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.HIWAY_MIN WHERE ID = ''{0}'' AND TM = TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'')");
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