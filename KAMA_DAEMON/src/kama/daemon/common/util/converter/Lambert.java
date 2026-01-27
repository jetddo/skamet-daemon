package kama.daemon.common.util.converter;

/**
 * @author chlee
 * Created on 2017-01-10.
 */
public class Lambert
{
    // 격자 파라메터 정의값
    // 1. RDPS_SGL_EQVPTL: 기존 포트란 코드의 상당온위값 가져올때 정의된 정보
    public enum GRID_PARAM_TYPE { RDAPS_SGL_EQVPTL, RDR, RDR2, RDR_KMA_HCI, RDR_KMA_HAIL, RDR_KMA_MAPLE, RDR_KMA_MOTION, RDR_KMA_WD, RDR_KMA_HSR }

    private static final double PI = Math.asin(1) * 2;
    private static final double DEGRAD = PI / 180.0;
    private static final double RADDEG = 180.0 / PI;
    private double SN, SF, RO;
    private lamc_parameter _m_map;

    public static class lamc_parameter implements Cloneable
    {
        // 기존 변수: Re, grid, slat1, slat2, olat, olon, xo, yo
        public double Re = 6371.00877; // Earth radius (km) [사용할 지구 반경]
        public double grid = 6.0; // for 6 km [격자간격] // LC 1.5 // 6
        public double slat1 = 30.0; // Standard latitude 1 [표준 위도1] (최하단 위도)
        public double slat2 = 60.0; // Standard latitude 2 [표준 위도2] // TRUE_LAT_NORTH (최상단 위도)
        public double center_lat = 38.0; // latitude of known point in map (degree) [기준점의 위도]
        public double center_lon = 126.0; // longitude of known point in map (degree) [기준점의 경도]
        public double xo = 66.0; // for 6 km [기준점의 X좌표] // 262 (평면좌표의 x 길이를 반으로 나눈 값)
        public double yo = 105.0; // for 6 km [기준점의 Y좌표] // 419 (평면좌표의 y 길이를 반으로 나눈 값)

        @Override
        public lamc_parameter clone()
        {
            lamc_parameter copy;

            copy = new lamc_parameter();
            copy.Re = Re;
            copy.grid = grid;
            copy.slat1 = slat1;
            copy.center_lat = center_lat;
            copy.center_lon = center_lon;
            copy.xo = xo;
            copy.yo = yo;

            return copy;
        }
    }

    /**
     * 사전에 정의된 격자 정보를 사용하여 클래스 생성
     * @param gridParamType
     */
    public Lambert(GRID_PARAM_TYPE gridParamType)
    {
        _init(gridParamType);
    }

    /**
     * 따로 정의된 격자값을 받아 클래스 생성
     * @param map
     */
    public Lambert(lamc_parameter map)
    {
        _init(map);
    }

    private static class WrapperDouble
    {
        public WrapperDouble()
        {
            value = 0;
        }

        public WrapperDouble(double val)
        {
            value = val;
        }

        public double value;
    }

    /**
     * 사전에 정의된 격자 정의값을 사용
     * @param gridParamType 격자 타입
     */
    private lamc_parameter _getPredefinedGridValue(GRID_PARAM_TYPE gridParamType)
    {
        lamc_parameter m_map = new lamc_parameter();

        if (gridParamType == GRID_PARAM_TYPE.RDAPS_SGL_EQVPTL)
        {
            m_map.Re = 6371.00877; // Earth radius (km) [사용할 지구 반경]
            m_map.grid = 6.0; // for 6 km [격자간격] // LC 1.5 // 6
            m_map.slat1 = 30.0; // Standard latitude 1 [표준 위도1] (최하단 위도)
            m_map.slat2 = 60.0; // Standard latitude 2 [표준 위도2] // TRUE_LAT_NORTH (최상단 위도)
            m_map.center_lat = 38.0; // latitude of known point in map (degree) [기준점의 위도]
            m_map.center_lon = 126.0; // longitude of known point in map (degree) [기준점의 경도]
            m_map.xo = 66.0; // for 6 km [기준점의 X좌표] // 262 (평면좌표의 x 길이를 반으로 나눈 값)
            m_map.yo = 105.0; // for 6 km [기준점의 Y좌표] // 419 (평면좌표의 y 길이를 반으로 나눈 값)
        }
        else if (gridParamType == GRID_PARAM_TYPE.RDR)
        {
            m_map.Re = 6371.00877f; // 사용할 지구반경, km
            m_map.grid = 1.0f; // 격자간격, km
            m_map.slat1 = 30.0f; // 표준위도, degree
            m_map.slat2 = 60.0f; // 표준위도, degree
            m_map.center_lat = 38.0f; // 기준점의 경도, degree
            m_map.center_lon = 126.0f; // 기준점의 위도, degree
            m_map.xo = 366f; // 기준점의 x좌표, 격자거리
            m_map.yo = 771f; // 시작여부 (0 = 시작)
        }
        else if (gridParamType == GRID_PARAM_TYPE.RDR2)
        {
            m_map.Re = 6371.00877f; // 사용할 지구반경, km
            m_map.grid = 1.0f; // 격자간격, km
            m_map.slat1 = 30.0f; // 표준위도, degree
            m_map.slat2 = 60.0f; // 표준위도, degree
            m_map.center_lat = 38.0f; // 기준점의 경도, degree
            m_map.center_lon = 126.0f; // 기준점의 위도, degree
            m_map.xo = 400; // 기준점의 x좌표, 격자거리
            m_map.yo = 789; // 시작여부 (0 = 시작)
        }
        else if (gridParamType == GRID_PARAM_TYPE.RDR_KMA_HCI)
        {
            m_map.Re = 6371.00877f; // 사용할 지구반경, km
            m_map.grid = 0.5f; // 격자간격, km
            m_map.slat1 = 30.0f; // 표준위도, degree
            m_map.slat2 = 60.0f; // 표준위도, degree
            m_map.center_lat = 38.0f; // 기준점의 경도, degree
            m_map.center_lon = 126.0f; // 기준점의 위도, degree
            m_map.xo = 881; // 기준점의 x좌표, 격자거리
            m_map.yo = 1541; // 시작여부 (0 = 시작)
        }
        else if (gridParamType == GRID_PARAM_TYPE.RDR_KMA_HAIL)
        {
            m_map.Re = 6371.00877f; // 사용할 지구반경, km
            m_map.grid = 0.5f; // 격자간격, km
            m_map.slat1 = 30.0f; // 표준위도, degree
            m_map.slat2 = 60.0f; // 표준위도, degree
            m_map.center_lat = 38.0f; // 기준점의 경도, degree
            m_map.center_lon = 126.0f; // 기준점의 위도, degree
            m_map.xo = 1120; // 기준점의 x좌표, 격자거리
            m_map.yo = 1680; // 시작여부 (0 = 시작)
        }
        else if (gridParamType == GRID_PARAM_TYPE.RDR_KMA_MAPLE)
        {
            m_map.Re = 6371.00877f; // 사용할 지구반경, km
            m_map.grid = 1f; // 격자간격, km
            m_map.slat1 = 30.0f; // 표준위도, degree
            m_map.slat2 = 60.0f; // 표준위도, degree
            m_map.center_lat = 38.0f; // 기준점의 경도, degree
            m_map.center_lon = 126.0f; // 기준점의 위도, degree
            m_map.xo = 441; // 기준점의 x좌표, 격자거리
            m_map.yo = 771; // 시작여부 (0 = 시작)
        }
        else if (gridParamType == GRID_PARAM_TYPE.RDR_KMA_MOTION)
        {
            m_map.Re = 6371.00877f; // 사용할 지구반경, km
            m_map.grid = 1f; // 격자간격, km
            m_map.slat1 = 30.0f; // 표준위도, degree
            m_map.slat2 = 60.0f; // 표준위도, degree
            m_map.center_lat = 38.0f; // 기준점의 경도, degree
            m_map.center_lon = 126.0f; // 기준점의 위도, degree
            m_map.xo = 441; // 기준점의 x좌표, 격자거리
            m_map.yo = 771; // 시작여부 (0 = 시작)
        }
        else if (gridParamType == GRID_PARAM_TYPE.RDR_KMA_WD)
        {
            m_map.Re = 6371.00877f; // 사용할 지구반경, km
            m_map.grid = 1f; // 격자간격, km
            m_map.slat1 = 30.0f; // 표준위도, degree
            m_map.slat2 = 60.0f; // 표준위도, degree
            m_map.center_lat = 38.0f; // 기준점의 경도, degree
            m_map.center_lon = 126.0f; // 기준점의 위도, degree
            m_map.xo = 440; // 기준점의 x좌표, 격자거리
            m_map.yo = 700; // 시작여부 (0 = 시작)
        }
        else if (gridParamType == GRID_PARAM_TYPE.RDR_KMA_HSR)
        {
            m_map.Re = 6371.00877f; // 사용할 지구반경, km
            m_map.grid = 0.5f; // 격자간격, km
            m_map.slat1 = 30.0f; // 표준위도, degree
            m_map.slat2 = 60.0f; // 표준위도, degree
            m_map.center_lat = 38.0f; // 기준점의 경도, degree
            m_map.center_lon = 126.0f; // 기준점의 위도, degree
            m_map.xo = 1121; // 기준점의 x좌표, 격자거리
            m_map.yo = 1681; // 시작여부 (0 = 시작)
        }

        return m_map;
    }

    /**
     * 격자정보 생성 (초기화)
     */
    private void _init(GRID_PARAM_TYPE gridParamType)
    {
        lamc_parameter m_map;
        m_map = _getPredefinedGridValue(gridParamType);

        _init(m_map);
    }

    /**
     * 격자정보 생성 (초기화)
     * @param baseParam 기초 파라메터 정보
     */
    private void _init(lamc_parameter baseParam)
    {
        _m_map = baseParam.clone();
        _m_map.slat1 *= DEGRAD;
        _m_map.slat2 *= DEGRAD;
        _m_map.center_lat *= DEGRAD;
        _m_map.center_lon *= DEGRAD;
        _m_map.Re /= _m_map.grid;

        if (_m_map.slat1 == _m_map.slat2 || Math.abs(_m_map.slat1) >= PI * 0.5 || Math.abs(_m_map.slat2) >= PI * 0.5)
        {
            throw new RuntimeException(String.format("ERROR [ LAMCPROJ ]"));
        }

        // initialize
        SN = Math.tan(PI * 0.25 + _m_map.slat2 * 0.5) / Math.tan(PI * 0.25 + _m_map.slat1 * 0.5);
        SN = Math.log(Math.cos(_m_map.slat1) / Math.cos(_m_map.slat2)) / Math.log(SN);
        SF = (Math.pow((Math.tan(PI * 0.25 + _m_map.slat1 * 0.5)), SN) * Math.cos(_m_map.slat1)) / SN;

        if (Math.abs(_m_map.center_lat) > 89.9 * DEGRAD)
        {
            if (SN * _m_map.center_lat < 0.0)
            {
                throw new RuntimeException("ERROR [ LAMCPROJ ]");
            }

            RO = 0.0;
        }
        else
        {
            RO = _m_map.Re * SF / Math.pow(Math.tan(PI * 0.25 + _m_map.center_lat * 0.5), SN);
        }

        _getPredefinedGridValue(GRID_PARAM_TYPE.RDAPS_SGL_EQVPTL);
    }

    /**
     * XY 좌표를 위경도 값으로 바꿔줌
     * @param x x 좌표
     * @param y y 좌표
     * @return lat, lon 배열 (순서대로)
     */
    public PointF lambertToWgs84(double x, double y)
    {
        PointF pointF;
        WrapperDouble clsLon, clsLat;
        clsLon = new WrapperDouble(0);
        clsLat = new WrapperDouble(0);

        x = x - 1;
        y = y - 1;
        lamcproj(clsLat, clsLon, new WrapperDouble(x), new WrapperDouble(y), 1);

        pointF = new PointF((float)clsLon.value, (float)clsLat.value);

        //return new double[] { clsLat.value, clsLon.value };
        return pointF;
    }

    /**
     * 위경도 좌표를 XY 좌표로 바꿔줌
     * @param lat 위도
     * @param lon 경도
     * @return x, y 배열 (순서대로)
     */
    public PointF wgs84ToLambert(double lat, double lon)
    {
        PointF pointF;
        WrapperDouble clsX, clsY;
        clsX = new WrapperDouble(0);
        clsY = new WrapperDouble(0);

        lamcproj(new WrapperDouble(lat), new WrapperDouble(lon), clsX, clsY, 0);
        clsX.value = (int)(clsX.value + 1.5);
        clsY.value = (int)(clsY.value + 1.5);

        pointF = new PointF((float)clsX.value, (float)clsY.value);

        //return new double[] { clsX.value, clsY.value };
        return pointF;
    }

    private void lamcproj(WrapperDouble W_ALAT, WrapperDouble W_ALON, WrapperDouble W_X, WrapperDouble W_Y, int N)
    {
        double ALAT, ALON, X, Y;
        double RA, THETA, XN, YN, RN;

        ALAT = W_ALAT.value;
        ALON = W_ALON.value;
        X = W_X.value;
        Y = W_Y.value;

        // Convert
        if (N == 0)
        {
            if (Math.abs(ALAT) > 89.9)
            {
                if (SN * ALAT < 0.0)
                {
                    throw new RuntimeException("ERROR");
                }

                RA = 0;
            }
            else
            {
                RA = _m_map.Re * SF / Math.pow(Math.tan(PI * 0.25 + ALAT * DEGRAD * 0.5), SN);
            }

            THETA = ALON * DEGRAD - _m_map.center_lon;

            if (THETA > PI)
            {
                THETA = THETA - 2.0 * PI;
            }

            if (THETA < -PI)
            {
                THETA = 2.0 * PI + THETA;
            }

            THETA = SN * THETA;
            X = RA * Math.sin(THETA) + _m_map.xo;
            Y = RO - RA * Math.cos(THETA) + _m_map.yo;

            W_X.value = X;
            W_Y.value = Y;
        }
        else if (N == 1)
        {

            XN = X - _m_map.xo;
            YN  = RO - Y + _m_map.yo;
            RA = Math.sqrt(XN * XN + YN * YN);

            if (SN < 0)
            {
                // if (sn < 0.0) - ra; // @@ 이건 무슨 코드지?
                throw new RuntimeException("Invalid SN");
            }

            ALAT = Math.pow((_m_map.Re * SF / RA), (1.0 / SN));
            ALAT = 2.0 * Math.atan(ALAT) - PI * 0.5;

            if (Math.abs(XN) <= 0.0)
            {
                THETA = 0.0;
            }
            else
            {
                if (Math.abs(YN) <= 0.0)
                {
                    THETA = PI * 0.5;

                    if (XN < 0.0)
                    {
                        //if( xn < 0.0 ) - theta; // @@ 이건 무슨 코드지? 2
                        throw new RuntimeException("Invalid XN");
                    }
                }
                else
                {
                    THETA = Math.atan2(XN, YN);
                }
            }

            ALON = THETA / SN + _m_map.center_lon;
            ALAT *= RADDEG;
            ALON *= RADDEG;

            W_ALAT.value = ALAT;
            W_ALON.value = ALON;
        }
    }
}