package kama.daemon.model.prediction.adopt.WAFC;

import com.mchange.v2.log.log4j.Log4jMLog;
import org.apache.commons.logging.impl.Log4JLogger;
import ucar.ma2.Array;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author chlee
 * Created on 2017-01-24.
 * WAFC 관련 데이터 파싱하는 클래스.
 * 예시파일: 20161009_0600f36.grib2
 */
public class WAFCLoader
{
    private static final int WIDTH = 288, HEIGHT = 145;
    private static final double GRID_UNIT_X = 1.25, GRID_UNIT_Y = 1.25;
    private static final double AXIS_MIN_Y = -90.0, AXIS_MAX_Y = 90.0;
    private static final double AXIS_MIN_X = 0.0, AXIS_MAX_X = 358.75;
    private WAFC_Data _wafc_data;

    private class WAFC_Data
    {
        public float[] tropo_heights;
    }

    public WAFCLoader(String filePath) throws IOException
    {
        NetcdfFile ncFile = NetcdfFile.open(new File(filePath).getAbsolutePath());

        Variable var_tropo_height = ncFile.findVariable("ICAO_Standard_Atmosphere_Reference_Height_tropopause");

        Array data = var_tropo_height.read();

        _wafc_data = new WAFC_Data();

        float[] dblData = (float[])data.getStorage();

        _wafc_data.tropo_heights = dblData;
    }

    /**
     * 특정 좌표에 대한 값을 xy 좌표값으로 변환
     * @param lat 위도
     * @param lon 경도
     * @return 0: y 좌표, 1: x 좌표
     */
    private static int[] convertLonLatToArrayIndexes(double lat, double lon)
    {
        int gridX, gridY;
        int x_coord, y_coord;
        int[] indexes;

        indexes = new int[2];

        gridY = (int)(HEIGHT / GRID_UNIT_Y);
        gridX = (int)(WIDTH / GRID_UNIT_X);

        // 격자 계산
        y_coord = (int)((HEIGHT - 1) - ((lat - AXIS_MIN_Y) / GRID_UNIT_Y));
        x_coord = (int)(lon / GRID_UNIT_X);

        indexes[0] = y_coord; // lat
        indexes[1] = x_coord; // lon

        return indexes;
    }

    /**
     * 특정 좌표에 대한 대류권계면 값을 받아옴
     * @param lat 위도
     * @param lon 경도
     * @return 대류권계면 값
     */
    public double getTropoHeight(double lat, double lon)
    {
        int[] indexes;

        indexes = convertLonLatToArrayIndexes(lat, lon);

        return _wafc_data.tropo_heights[indexes[0] * WIDTH + indexes[1]];
    }
}
