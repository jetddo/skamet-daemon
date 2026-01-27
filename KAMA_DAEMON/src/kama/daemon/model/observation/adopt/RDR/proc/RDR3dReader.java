package kama.daemon.model.observation.adopt.RDR.proc;

import kama.daemon.common.util.GZipTgzReader;
import kama.daemon.common.util.Log;
import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.PointF;
import kama.daemon.model.observation.adopt.RDR.data.CROSS_ROUTES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by chlee on 2017-02-23.
 */
public class RDR3dReader
{
    public static final int XDIM = 960;
    public static final int YDIM = 1200;
    private static final int DIMENSION_SIZE = XDIM * YDIM;

    private ArrayList<RDR3dLayer> _m_layers;
    private Lambert _lambert;

    /**
     * 레이더 reader 생성자
     * @param gZipFilePath 레이더 데이터 파일 경로
     * @throws IOException
     */
    public RDR3dReader(String gZipFilePath) throws IOException
    {
        GZipTgzReader gZipTgzReader;

        gZipTgzReader = new GZipTgzReader(gZipFilePath);

        byte[] array = gZipTgzReader.readAllBytes();

        _init(array);
    }

    /**
     * 바이트 배열로 오브젝트 생성
     * @param buffer
     */
    public RDR3dReader(byte[] buffer)
    {
        _init(buffer);
    }

    /**
     * 생성자 내부 공통 부분
     * @param buffer 데이터 바이트 배열
     */
    private void _init(byte[] buffer)
    {
        RDR3dLayer rdr3dLayer;
        _m_layers = new ArrayList<>();

        _lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDR2);

        for (int i = 0; i < buffer.length; i += (XDIM * YDIM))
        {
            rdr3dLayer = RDR3dLayer.fromBinary(XDIM, YDIM, Arrays.copyOfRange(buffer, i, i + (XDIM * YDIM)));
            _m_layers.add(rdr3dLayer);
        }
    }

    public void regridAndSmoothAllLayers()
    {
        RDR3dLayer[] layers = new RDR3dLayer[_m_layers.size()];

        layers = _m_layers.toArray(layers);

        for (int i = 0; i < layers.length; i++)
        {
            layers[i] = layers[i].performSmoothing();
            layers[i] = layers[i].performRegriding();
        }

        _m_layers = new ArrayList<>(Arrays.asList(layers));
    }

    /**
     * 레이어 개수 반환
     * @return RDR 레이어 개수
     */
    public int layerCount()
    {
        if (_m_layers == null)
        {
            return -1;
        }

        return _m_layers.size();
    }

    /**
     * 전체 레이어 반환
     * @return 전체 레이어 배열
     */
    public RDR3dLayer[] getLayers()
    {
        RDR3dLayer[] layers;
        layers = new RDR3dLayer[_m_layers.size()];

        return _m_layers.toArray(layers);
    }

    /**
     * 특정 레이어를 반환
     * @param index 반환할 레이어 인덱스
     * @return index에 맞는 해당 레이어
     */
    public RDR3dLayer getLayer(int index)
    {
        return _m_layers.get(index);
    }

    /**
     * 항로에 따른 연진단면 생성
     * @param routeName 항로 이름
     * @param width_pixel 반환할 이미지의 가로 픽셀
     * @return 생성된 연직단면
     */
    public RDR3dLayer getVerticalSlice(String routeName, int width_pixel)
    {
        CROSS_ROUTES routeInfo = CROSS_ROUTES.getInstance();
        PointF[] route = routeInfo.retrieveRoute(routeName);

        return getVerticalSlice(route, width_pixel);
    }

    /**
     * 경위도값을 배열로 받아 연직단면 생성
     * @param points 경위도값 배열
     * @return 연직단면
     */
    public RDR3dLayer getVerticalSlice(PointF[] points, int width_pixel)
    {
        double totalDistance = 0;

        for (int i = 0; i < points.length - 1; i++)
        {
            totalDistance += GeoUnitCalculator.distanceBetweenTwoCoordinates(points[i], points[i + 1]).distance;
        }

        totalDistance = Math.ceil(totalDistance);

        double unit = totalDistance / width_pixel;

        RDR3dLayer layer = new RDR3dLayer(width_pixel, 81);
        int x_total = 0;

        for (int i = 0; i < points.length - 1; i++)
        {
            RDR3dLayer partial;
            partial = getVerticalSliceInternal(points[i], points[i + 1], unit);

            for (int j = 0; j < partial.getSizeX(); j++)
            {
                layer.setVerticalValues(x_total++, partial.getVertical(j));
            }
        }

        return layer;
    }

    /**
     * 두 개의 경위도 지점을 받아 직선거리에 대한 연직단면 생성
     * @param c1 시작 경위도
     * @param c2 끝 경위도
     * @return 연직단면
     */
    public RDR3dLayer getVerticalSlice(PointF c1, PointF c2, int width_pixel)
    {
        double totalDistance = GeoUnitCalculator.distanceBetweenTwoCoordinates(c1, c2).distance;
        double unit = Math.ceil(totalDistance) / width_pixel;

        return getVerticalSliceInternal(c1, c2, unit);
    }

    /**
     * 연직단면 생성의 내부 함수
     * @param c1 연직단면을 생성할 시작 경위도 지점
     * @param c2 연직단면을 생성할 끝 경위도 지점
     * @param unit 레이더 이미지 격자 간격 (e.g., n km 간격으로 값을 표출)
     * @return 레이더 연직 단면
     */
    private RDR3dLayer getVerticalSliceInternal(PointF c1, PointF c2, double unit)
    {
        GeoUnitCalculator.DistanceInfo distanceInfo = GeoUnitCalculator.distanceBetweenTwoCoordinates(c1, c2);

        int numXGrids = (int)(Math.round(distanceInfo.distance / unit));
        RDR3dLayer layer = new RDR3dLayer(numXGrids, _m_layers.size());

        for (int i = 1; i <= numXGrids; i++)
        {
            PointF destPoint;
            destPoint = GeoUnitCalculator.calculateDestination(c1, unit * i, distanceInfo.bearing);

            int[] values = getVerticalSlice(destPoint);

            if (values != null)
            {
                layer.setVerticalValues(i - 1, values);
            }

            // 디버그시 로그 print.
            Log.print(Log.LOG_TYPE.DEBUG, String.format("%f, %f", destPoint.getY(), destPoint.getX()));
        }

        return layer;
    }

    /**
     * 하나의 경위도 지점에 대한 연직단면 생성
     * @param c 경위도 지점
     * @return 연진단면 값 1차원 배열
     */
    private int[] getVerticalSlice(PointF c)
    {
        PointF pointF = _lambert.wgs84ToLambert(c.y, c.x);

        return getVerticalSlice((int)pointF.x, (int)pointF.y);
    }

    /**
     * 하나의 점에 대한 연직 단면도 추출 (경위도가 아닌 이미지상의 x, y 좌표 값)
     * @param x x좌표값
     * @param y y 좌표값
     * @return 연직단면 값 1차원 배열
     */
    public int[] getVerticalSlice(int x, int y)
    {
        int[] values;
        int arrayLength;
        values = new int[_m_layers.size()];
        arrayLength = values.length;

        // 가장 최상층과 최하층은 레이더 값이 아니므로 제외
        for (int i = 1; i < _m_layers.size() - 1; i++)
        {
            if (_m_layers.get(i).isValidCoordinate(x, y))
            {
                values[i] = _m_layers.get(i).getValue(x, y);
            }
            else
            {
                return null;
            }
        }

        return values;
    }
}
