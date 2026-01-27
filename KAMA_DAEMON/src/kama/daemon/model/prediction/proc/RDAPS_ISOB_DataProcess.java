package kama.daemon.model.prediction.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.model.prediction.adopt.RDAPS_ISOB.loader.VHumidityCalculator;
import kama.daemon.model.prediction.adopt.RDAPS_ISOB.loader.VerticalVisibilityLoader;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * @author chlee
 * Created on 2017-01-16.
 */
public class RDAPS_ISOB_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "rdaps_isob";
    private static final int DB_COLUMN_COUNT = 12;
    private static final int FILE_DATE_INDEX_POS = 3; // r120_city_grph_korea.2017031318
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2, 3 }; // STN_ID, PREDCT_TIME, MDL_PROD_TIME, FLOOR
    private final int INSERT_QUERY = 1;
    private final int TRUNCATE_QUERY = 2;
    private final int DELETE_QUERY = 3;

    public RDAPS_ISOB_DataProcess(DaemonSettings settings)
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
        VerticalVisibilityLoader loader = new VerticalVisibilityLoader(file.getAbsolutePath());

        // 양양공항(92), 김포공항(110), 인천공항(113), 청주공항(128), 울산공항(151), 무안공항(163), 여수공항(167), 김해공항(153), 제주공항(182)
        // 자료 정리 엑셀파일 참조
        int[] stnIDs = { 47092, 47110, 47113, 47128, 47151, 47153, 47163, 47166, 47167, 47182 };

        //
        // 2017/01/17
        // 참고: 추후 데이터 칼럼 추가하거나, 일부 데이터만 다른 파일로 교체될 수 있고,
        //       VerticalVisibilityLoader에서 데이터를 3가지의 서로 다른 배열로 값을 주기에,
        //       그에 맞추어 MERGE Query 로 작성하였음
        //

        // RDAPS_ISOB 테이블 처리
        for (int stnID : stnIDs)
        {
            processTemperatureData(dbManager, file, processorInfo, loader, stnID);
            processWindData(dbManager, file, processorInfo, loader, stnID);
            processCloudInfoData(dbManager, file, processorInfo, loader, stnID);
        }

        dbManager.commit();

        // RDAPS_SGL 테이블 처리
        for (int stnID : stnIDs)
        {
            processSGLRainInfoData(dbManager, file, processorInfo, loader, stnID);
        }
    }

    //<editor-fold desc="내부 처리 함수">
    /**
     * 연직시계열 loader로부터 강수와 관련된 부분 레코드 업데이트
     * @param dbManager
     * @param file
     * @param processorInfo
     * @param loader
     * @param stationID
     */
    private void processSGLRainInfoData(DatabaseManager dbManager, File file, ProcessorInfo processorInfo, VerticalVisibilityLoader loader, int stationID)
    {
        double[] rain_info_values = loader.getRainInfoListByTime(stationID);
        double[] humidity_values = loader.getRelativeHumidityListByTime(stationID);
        double[] surface_pres_values = loader.getSurfacePressureListByTime(stationID);
        double[] wind_spd_values = loader.getWindSpeedListByTime(stationID);
        double[] wind_gust_spd_values = loader.getWindGSpeedListByTime(stationID);
        double[] wind_u_values = loader.getWindUSpeedListByTime(stationID);
        double[] wind_v_values = loader.getWindVSpeedListByTime(stationID);
        double[] lowest_sigma_temp_values = loader.getLowestSigmaHTemperatureListByTime(stationID);

        // 값이 없을 경우, 그냥 함수 리턴
        if (rain_info_values == null || humidity_values == null ||
                surface_pres_values == null || wind_spd_values == null ||
                wind_spd_values == null || wind_gust_spd_values == null ||
                wind_gust_spd_values == null || wind_u_values == null ||
                wind_u_values == null || wind_v_values == null ||
                lowest_sigma_temp_values == null)
        {
            return;
        }

        String queryTemplate = "MERGE INTO %%WORKSPACE%%.NMDL_RDAPS_SGL d USING (SELECT ''{0}'' STN_ID, TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') PREDCT_TIME, TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'') MDL_PROD_TIME FROM dual) s ON (d.STN_ID = s.STN_ID AND d.PREDCT_TIME = s.PREDCT_TIME AND d.MDL_PROD_TIME = s.MDL_PROD_TIME) WHEN MATCHED THEN UPDATE SET d.WIND_DIR = ''{3}'', d.WIND_SPD = ''{4}'', d.WIND_GUST_SPD = ''{5}'', d.WIND_VECTOR_V = ''{6}'', d.WIND_VECTOR_U = ''{7}'', d.LWST_SIGMA_TEMP = ''{8}'', d.HUMID_VV = ''{9}'', d.SURFACE_PRES = ''{10}'', d.PRECIP = ''{11}''  WHEN NOT MATCHED THEN INSERT(STN_ID, PREDCT_TIME, MDL_PROD_TIME, WIND_DIR, WIND_SPD, WIND_GUST_SPD, WIND_VECTOR_V, WIND_VECTOR_U, LWST_SIGMA_TEMP, HUMID_VV, SURFACE_PRES, PRECIP) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'')";

        for (int time = 0; time < rain_info_values.length; time++)
        {
            String query;
            double wind_dir;
            int index = 0;

            wind_dir = (270 - Math.atan2(wind_v_values[time], wind_u_values[time]) * 180 / Math.PI) % 360;

            // STN_ID, PREDCT_TIME, MDL_PROD_TIME (PK)
            // WIND_DIR, WIND_SPD, WIND_GUST_SPD, WIND_VECTOR_V, WIND_VECTOR_U, LWST_SIGMA_TEMP, HUMID_VV, SURFACE_PRES, PRECIP (data)
            Object[] bindArray;
            bindArray = new Object[12];
            bindArray[index++] = convertToDBText(stationID - 47000);
            bindArray[index++] = convertToDBText(DateUtils.addHours(processorInfo.FileDateFromNameKST, time * 3));
            bindArray[index++] = convertToDBText(processorInfo.FileDateFromNameUTC);
            bindArray[index++] = convertToDBText(wind_dir);
            bindArray[index++] = convertToDBText(wind_spd_values[time]);
            bindArray[index++] = convertToDBText(wind_gust_spd_values[time]);
            bindArray[index++] = convertToDBText(wind_v_values[time]);
            bindArray[index++] = convertToDBText(wind_u_values[time]);
            bindArray[index++] = convertToDBText(lowest_sigma_temp_values[time]);
            bindArray[index++] = convertToDBText(humidity_values[time]);
            bindArray[index++] = convertToDBText(surface_pres_values[time]);
            bindArray[index++] = convertToDBText(rain_info_values[time]);

            // merge query (insert)
            query = MessageFormat.format(makeQuery(queryTemplate), bindArray);
            dbManager.executeUpdate(query);
        }
    }

    /**
     * 연직시계열 loader로부터 바람과 관련된 부분 레코드 업데이트
     * @param dbManager
     * @param file
     * @param processorInfo
     * @param loader
     * @param stationID
     */
    private void processWindData(DatabaseManager dbManager, File file, ProcessorInfo processorInfo, VerticalVisibilityLoader loader, int stationID)
    {
        double[][] u_values = loader.getWindUListByPresAndTime(stationID);
        double[][] v_values = loader.getWindVListByPresAndTime(stationID);

        // 값이 없을 경우, 그냥 함수 리턴
        if (u_values == null || v_values == null)
        {
            return;
        }

        String queryTemplate = "MERGE INTO %%WORKSPACE%%.NMDL_RDAPS_ISOB d USING (SELECT ''{0}'' STN_ID, TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') PREDCT_TIME, TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'') MDL_PROD_TIME, ''{3}'' FLOOR FROM dual) s ON (d.STN_ID = s.STN_ID AND d.PREDCT_TIME = s.PREDCT_TIME AND d.MDL_PROD_TIME = s.MDL_PROD_TIME AND d.FLOOR = s.FLOOR) WHEN MATCHED THEN UPDATE SET d.ALT = ''{4}'', d.WIND_DIR = ''{5}'', d.WIND_SPD = ''{6}'' WHEN NOT MATCHED THEN INSERT(STN_ID, PREDCT_TIME, MDL_PROD_TIME, FLOOR, ALT, WIND_DIR, WIND_SPD) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'', ''{4}'', ''{5}'', ''{6}'')";

        for (int floorLevel = 0; floorLevel < u_values.length; floorLevel++)
        {
            for (int time = 0; time < u_values[floorLevel].length; time++)
            {
                double windDir, windSpd;
                double altitudeMeter;
                Object[] bindArray;
                String query;
                int index = 0;

                // 고도값 계산
                altitudeMeter = VerticalVisibilityLoader.floorIndexToAltitudeMeter(floorLevel);

                // 풍향, 풍속값 계산
                windDir = (270 - Math.atan2(v_values[floorLevel][time], u_values[floorLevel][time]) * 180 / Math.PI) % 360;
                windSpd = Math.sqrt(Math.pow(u_values[floorLevel][time], 2) + Math.pow(v_values[floorLevel][time], 2));

                // 데이터: 지점 ID, 예측 시각 (KST), 모델 생산 시각 (UTC), 등압면 층, 풍향, 풍속
                //STN_ID, PREDCT_TIME, MDL_PROD_TIME, FLOOR, WIND_DIR, WIND_SPD
                bindArray = new Object[7];
                bindArray[index++] = convertToDBText(stationID - 47000);
                bindArray[index++] = convertToDBText(DateUtils.addHours(processorInfo.FileDateFromNameKST, time * 3));
                bindArray[index++] = convertToDBText(processorInfo.FileDateFromNameUTC);
                bindArray[index++] = convertToDBText(floorLevel);
                bindArray[index++] = convertToDBText(altitudeMeter);
                bindArray[index++] = convertToDBText(windDir);
                bindArray[index++] = convertToDBText(windSpd);

                query = MessageFormat.format(makeQuery(queryTemplate), bindArray);
                dbManager.executeUpdate(query);

                //query = MessageFormat.format(queryTemplate, stationID, )
            }
        }
    }

    /**
     * 연직시계열 loader로부터 구름과 관련된 부분 레코드 업데이트
     * @param dbManager
     * @param file
     * @param processorInfo
     * @param loader
     * @param stationID
     */
    private void processCloudInfoData(DatabaseManager dbManager, File file, ProcessorInfo processorInfo, VerticalVisibilityLoader loader, int stationID)
    {
        double[][] ice_values = loader.getCloudIceInfoListByPresAndTime(stationID);
        double[][] water_values = loader.getCloudWaterInfoListByPresAndTime(stationID);

        // 값이 없을 경우, 그냥 함수 리턴
        if (ice_values == null || water_values == null)
        {
            return;
        }

        for (int floorLevel = 0; floorLevel < ice_values.length; floorLevel++)
        {
            for (int time = 0; time < ice_values[floorLevel].length; time++)
            {
                String queryTemplate = "MERGE INTO %%WORKSPACE%%.NMDL_RDAPS_ISOB d USING (SELECT ''{0}'' STN_ID, TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') PREDCT_TIME, TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'') MDL_PROD_TIME, ''{3}'' FLOOR FROM dual) s ON (d.STN_ID = s.STN_ID AND d.PREDCT_TIME = s.PREDCT_TIME AND d.MDL_PROD_TIME = s.MDL_PROD_TIME AND d.FLOOR = s.FLOOR) WHEN MATCHED THEN UPDATE SET d.ALT = ''{4}'', d.CLOUD_ICE_INFO = ''{5}'', d.CLOUD_WTR_INFO = ''{6}'' WHEN NOT MATCHED THEN INSERT(STN_ID, PREDCT_TIME, MDL_PROD_TIME, FLOOR, ALT, CLOUD_ICE_INFO, CLOUD_WTR_INFO) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'', ''{4}'', ''{5}'', ''{6}'')";

                Object[] bindArray;
                String query;
                int index = 0;
                double altitudeMeter;

                // 고도값 계산
                altitudeMeter = VerticalVisibilityLoader.floorIndexToAltitudeMeter(floorLevel);

                bindArray = new Object[7];
                bindArray[index++] = convertToDBText(stationID - 47000);
                bindArray[index++] = convertToDBText(DateUtils.addHours(processorInfo.FileDateFromNameKST, time * 3));
                bindArray[index++] = convertToDBText(processorInfo.FileDateFromNameUTC);
                bindArray[index++] = convertToDBText(floorLevel);
                bindArray[index++] = convertToDBText(altitudeMeter);
                bindArray[index++] = convertToDBText(ice_values[floorLevel][time]);
                bindArray[index++] = convertToDBText(water_values[floorLevel][time]);

                query = MessageFormat.format(makeQuery(queryTemplate), bindArray);
                dbManager.executeUpdate(query);
            }
        }
    }

    /**
     * 연직시계열 loader로부터 온도, 기온과 관련된 부분 레코드 업데이트
     * @param dbManager
     * @param file
     * @param processorInfo
     * @param loader
     * @param stationID
     */
    private void processTemperatureData(DatabaseManager dbManager, File file, ProcessorInfo processorInfo, VerticalVisibilityLoader loader, int stationID)
    {
        VHumidityCalculator calc = new VHumidityCalculator(loader);
        double[][] taValues = loader.getTemperatureListByPresAndTime(stationID);

        // 값이 없을 경우, 그냥 함수 리턴
        if (taValues == null)
        {
            return;
        }

        String queryTemplate = "MERGE INTO %%WORKSPACE%%.NMDL_RDAPS_ISOB d USING (SELECT ''{0}'' STN_ID, TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') PREDCT_TIME, TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'') MDL_PROD_TIME, ''{3}'' FLOOR FROM dual) s ON (d.STN_ID = s.STN_ID AND d.PREDCT_TIME = s.PREDCT_TIME AND d.MDL_PROD_TIME = s.MDL_PROD_TIME AND d.FLOOR = s.FLOOR) WHEN MATCHED THEN UPDATE SET d.ALT = ''{4}'', d.TEMP = ''{5}'', d.EQV_POTL_TEMP = ''{6}'', d.DEW_PT = ''{7}'', d.DEW_PT_DPR = ''{8}'' WHEN NOT MATCHED THEN INSERT(STN_ID, PREDCT_TIME, MDL_PROD_TIME, FLOOR, ALT, TEMP, EQV_POTL_TEMP, DEW_PT, DEW_PT_DPR) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'')";

        for (int floorLevel = 0; floorLevel < taValues.length; floorLevel++)
        {
            for (int time = 0; time < taValues[floorLevel].length; time++)
            {
                Object[] bindArray;
                String query;
                int index = 0;
                double eqvPtlTemp;
                double pressureValue;
                double altitudeMeter;
                double dewPoint;

                // 고도값 계산
                altitudeMeter = VerticalVisibilityLoader.floorIndexToAltitudeMeter(floorLevel);

                // 상당온위값 계산
                pressureValue = VerticalVisibilityLoader.floorIndexToHectoPascal(floorLevel);
                eqvPtlTemp = (taValues[floorLevel][time] + 273.15) * Math.pow((1000 / pressureValue), 0.288) - 273.15;

                // 이슬점 온도 계산
                dewPoint = calc.calculateDewPointByFloorLv(stationID, time, floorLevel);

                // 데이터: 지점 ID, 예측 시각 (KST), 모델 생산 시각 (UTC), 등압면 층, 기온
                // STN_ID, PREDCT_TIME, MDL_PROD_TIME, FLOOR, TEMP
                bindArray = new Object[9];
                bindArray[index++] = convertToDBText(stationID - 47000);
                bindArray[index++] = convertToDBText(DateUtils.addHours(processorInfo.FileDateFromNameKST, time * 3));
                bindArray[index++] = convertToDBText(processorInfo.FileDateFromNameUTC);
                bindArray[index++] = convertToDBText(floorLevel);
                bindArray[index++] = convertToDBText(altitudeMeter);
                bindArray[index++] = convertToDBText(taValues[floorLevel][time]);
                bindArray[index++] = convertToDBText(eqvPtlTemp);
                bindArray[index++] = convertToDBText(dewPoint);
                bindArray[index++] = convertToDBText(taValues[floorLevel][time] - dewPoint);

                query = MessageFormat.format(makeQuery(queryTemplate), bindArray);
                dbManager.executeUpdate(query);

                //query = MessageFormat.format(queryTemplate, stationID, )
            }
        }
    }
    //</editor-fold>

    //<editor-fold desc="Auto-generated SQL queries">
    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.NMDL_RDAPS_ISOB(STN_ID, PREDCT_TIME, MDL_PROD_TIME, FLOOR, ALT, WIND_DIR, WIND_SPD, TEMP, EQV_POTL_TEMP, DEW_PT, DEW_PT_DPR, CLOUD_ICE_INFO, CLOUD_WTR_INFO) VALUES (''{0}'', TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss''), TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss''), ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'')");
        defineQueryFormat(TRUNCATE_QUERY, "DELETE FROM %%WORKSPACE%%.NMDL_RDAPS_ISOB");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.NMDL_RDAPS_ISOB WHERE STN_ID = ''{0}'' AND PREDCT_TIME = TO_DATE(''{1}'', ''YYYY-MM-DD HH24:mi:ss'') AND MDL_PROD_TIME = TO_DATE(''{2}'', ''YYYY-MM-DD HH24:mi:ss'') AND FLOOR = ''{3}''");
    }
    //</editor-fold>

    //<editor-fold desc="Auto-generated getters (No need to modify)">
    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
    //</editor-fold>

    /**
     * 데이터 파일 종류별 날짜 파싱 함수
     * @param file 데이터 파일
     * @return 날짜값
     * @throws ParseException
     */
    @Override
    protected Date parseDateFromFileName(File file) throws java.text.ParseException
    {
        return DateFormatter.parseDate(file.getName().split("\\.")[1], "yyyyMMddHH");
    }
}