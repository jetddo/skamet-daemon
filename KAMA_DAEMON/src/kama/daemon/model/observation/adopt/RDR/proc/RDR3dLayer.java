package kama.daemon.model.observation.adopt.RDR.proc;

import kama.daemon.common.util.Log;
import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.PointF;
import kama.daemon.model.observation.adopt.RDR.data.Layer2D;

import java.awt.*;
import java.io.File;
import java.io.IOException;

/**
 * Created by chlee on 2017-02-23.
 */
public class RDR3dLayer extends Layer2D implements Cloneable
{
    public RDR3dLayer(int width, int height)
    {
        super(width, height);
    }

    public RDR3dLayer(Layer2D layer2D)
    {
        super(layer2D);
    }

    /**
     * 바이너리 파일로부터 RDR 전체 레이어를 읽고, 오브젝트 생성하여 반환
     * @param width
     * @param height
     * @param buffer
     * @return
     */
    public static RDR3dLayer fromBinary(int width, int height, byte[] buffer)
    {
        Layer2D layer2D = byteArrayToLayer(width, height, buffer);

        return new RDR3dLayer(layer2D);
    }

    /**
     * 현재 레이어를 이미지 파일로 저장 (png)
     * @param file 저장할 파일
     * @throws IOException
     */
    public void saveAs(File file) throws IOException
    {
        saveAs(file, true);
    }

    /**
     * 현재 레이어를 이미지 파일로 저장
     * @param file 저장할 파일
     * @param flipVertical 상하 대칭 변경 필요시 true
     * @throws IOException
     */
    public void saveAs(File file, boolean flipVertical) throws IOException
    {
        RDR3dImageProcessor.createBitmapFromLayer(this, file, flipVertical);
    }

    public RDR3dLayer performSmoothing()
    {
        RDR3dLayer layer = this.clone();

        return smoothing(layer);
    }

    public RDR3dLayer performRegriding()
    {
        RDR3dLayer layer = this.clone();

        return regriding(layer);
    }

    /**
     * Byte buffer를 Layer2D (2차원 사각형)으로 변환하는 함수
     * @param readBuf
     * @return
     */
    private static Layer2D byteArrayToLayer(int XDIM, int YDIM, byte[] readBuf)
    {
        Layer2D layer2D = new Layer2D(XDIM, YDIM);

        for (int y = 0; y < YDIM; y++)
        {
            for (int x = 0; x < XDIM; x++)
            {
                layer2D.setValue(x, y, readBuf[(y * XDIM) + x]);
            }
        }

        return layer2D;
    }

    /**
     * 부정확한 좌표값 보정해주는 작업 (구현 필요)
     */
    private static RDR3dLayer regriding(RDR3dLayer layer)
    {
        Lambert lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDR2);
        RDR3dLayer data2 = new RDR3dLayer(layer.getSizeX(), layer.getSizeY());

        for (int x = 0; x < data2._sizeX; x++)
        {
            for (int y = 0; y < data2._sizeY; y++)
            {
                //data2.setValue(x, y, 50); // 빨간색 (for testing)
                data2.setValue(x, y, 0);
            }
        }

        // 최대 최소 위경도 찾기 : 같은 설정 좌표계면 한번만 해서 뽑아두면 됨
        double maxLat = -1;
        double maxLon = -1;
        double minLat = 91;
        double minLon = 181;

        Point minX = new Point();
        Point minY = new Point();
        Point maxX = new Point();
        Point maxY = new Point();

        for (int x = 0 ; x < layer._sizeX; x++)
        {
            for (int y = 0; y < layer._sizeY; y++)
            {
                PointF c = lambert.lambertToWgs84(x, y);

                if (c.x > maxLon)
                {
                    maxLon = c.x;
                    maxX.x = x;
                    maxX.y = y;
                }
                else if (c.x < minLon)
                {
                    minLon = c.x;
                    minX.x = x;
                    minX.y = y;
                }
                else if (c.y < minLat)
                {
                    minLat = c.y;
                    minY.x = x;
                    minY.y = y;
                }
                else if (c.y > maxLat)
                {
                    maxLat = c.y;
                    maxY.x = x;
                    maxY.y = y;
                }
            }
        }

        // 찾아낸 최대 최소 경위도값 표출 (사각형 격자의 가장 끝점을 알아낼때 사용)
        Log.print(Log.LOG_TYPE.DEBUG, String.format("%s : %s", maxLat, maxLon));
        Log.print(Log.LOG_TYPE.DEBUG, String.format("%s : %s", minLat, minLon));

        // 픽셀 당 간격
        double latTerm = (maxLat - minLat) / (layer._sizeY-1);
        double lonTerm = (maxLon - minLon) / (layer._sizeX-1);

        for(int x = 0; x < layer._sizeX; x++)
        {
            for (int y = 0; y < layer._sizeY; y++)
            {
                PointF coord = lambert.lambertToWgs84(x, y);
                int y2 = (int) ((coord.y - minLat) / latTerm);
                int x2 = (int) ((coord.x - minLon) / lonTerm);

                data2.setValue(x2, y2, layer.getValue(x, y));
            }
        }

        return data2;
    }

    /**
     * 레이어 스무딩 (급격한 값 변화 부분 완화) 하는 작업
     * @param rdr3dLayer 작업할 레이어
     * @return 작업된 레이어
     */
    private static RDR3dLayer smoothing(RDR3dLayer rdr3dLayer)
    {
        int XDIM, YDIM;
        float[] e = new float[4];
        float e1, e2;
        int sm_num = 2; // Smoothing 횟수

        int k, j, i;

        XDIM = rdr3dLayer.getSizeX();
        YDIM = rdr3dLayer.getSizeY();

        //
        // 중복 코드 제거 및 최적화 작업 필요
        //
        for (k = 0; k < sm_num; k++)
        {
            for (j = 0; j <= YDIM - 1; j++)
            {
                e1 = Layer2D.stou(rdr3dLayer.getByteValue(0, j)) * 100;
                e[0] = Layer2D.stou(rdr3dLayer.getByteValue(0, j)) * 100;
                e[1] = Layer2D.stou(rdr3dLayer.getByteValue(1, j)) * 100;

                for (i = 1; i < XDIM - 1; i++)
                {
                    e[2] = Layer2D.stou(rdr3dLayer.getByteValue(i + 1, j)) * 100;

                    if (e[0] > -1 && e[1] > -1 && e[2] > -1)
                    {
                        e2 = (e[0] + 2.0f * e[1] + e[2]) * 0.25f;
                    }
                    else if (e[0] > -1 && e[1] <= -1 && e[2] > -1)
                    {
                        e2 = (e[0] + e[2]) * 0.5f;
                    }
                    else
                    {
                        e2 = e[1];
                    }

                    rdr3dLayer.setValue(i - 1, j, Layer2D.utos((int)(e1 / 100)));
                    e1 = e2;
                    e[0] = e[1];
                    e[1] = e[2];
                }
                rdr3dLayer.setValue(i - 1, j, Layer2D.utos((int)(e1 / 100)));
            }

            for (i = 0; i <= XDIM - 1; i++)
            {
                e1 = Layer2D.stou(rdr3dLayer.getByteValue(i, 0)) * 100;
                e[0] = Layer2D.stou(rdr3dLayer.getByteValue(i, 0)) * 100;
                e[1] = Layer2D.stou(rdr3dLayer.getByteValue(1, i)) * 100;

                for (j = 1; j < YDIM - 1; j++)
                {
                    e[2] = Layer2D.stou(rdr3dLayer.getByteValue(i, j + 1)) * 100;

                    if (e[0] > -1 && e[1] > -1 && e[2] > -1)
                    {
                        e2 = (e[0] + 2.0f * e[1] + e[2]) * 0.25f;
                    }
                    else if (e[0] > -1 && e[1] <= -1 && e[2] > -1)
                    {
                        e2 = (e[0] + e[2]) * 0.5f;
                    }
                    else
                    {
                        e2 = e[1];
                    }

                    rdr3dLayer.setValue(i, j - 1, Layer2D.utos((int)(e1 / 100)));
                    e1 = e2;
                    e[0] = e[1];
                    e[1] = e[2];
                }

                rdr3dLayer.setValue(i, j - 1, Layer2D.utos((int)(e1 / 100)));
            }
        }

        return rdr3dLayer;
    }

    @Override
    public RDR3dLayer clone()
    {
        RDR3dLayer layer;
        layer = new RDR3dLayer(super.clone());

        return layer;
    }
}
