package kama.daemon.model.prediction.adopt.LDAPS_SGL.loader;

import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.PointF;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chlee
 * Created on 2017-01-09.
 */
public class EquivPotentialTempLoader
{
    private List<float[][]> _lstLayers;

    private final int WIDTH = 151, DEPTH = 199, HEIGHT = 31;
    private final int LAYER_COUNT = 42;

    /**
     * 클래스 생성자
     * @param filePath 데이터 파일 경로
     * @throws IOException
     */
    public EquivPotentialTempLoader(String filePath) throws IOException
    {
        _lstLayers = new ArrayList<float[][]>();
        _init(filePath);
    }

    /**
     * 클래스 초기화
     * @param filePath 데이터 파일 경로
     * @throws IOException
     */
    private void _init(String filePath) throws IOException
    {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r");
             FileChannel channel = raf.getChannel())
        {
            long nSize = raf.length();
            ByteBuffer buffer = ByteBuffer.allocateDirect((int) nSize).order(ByteOrder.BIG_ENDIAN);
            channel.read(buffer);
            buffer.clear();

            FloatBuffer dbuf = buffer.asFloatBuffer();
            float[][] dataBuf;

            dataBuf = new float[DEPTH][WIDTH];

            for (int n = 0; n < LAYER_COUNT; n++)
            {
                // 헤더 부분 잘라버림 (첫 레이어: 4 bytes, 두번째 레이어부터: 8 bytes. 이유는 모르겠음.)
                if (n == 0)
                {
                    dbuf.get();
                }
                else
                {
                    dbuf.get();
                    dbuf.get();
                }

                for (int y = 0; y < DEPTH; y++)
                {
                    for (int x = 0; x < WIDTH; x++)
                    {
                        dataBuf[y][x] = dbuf.get();
                    }
                }

                _lstLayers.add(dataBuf);
                dataBuf = new float[DEPTH][WIDTH];
            }
        }

        System.out.println("");
    }

    /**
     * 이미지 파일로 export (테스트용)
     * @param val xy 격자 배열 [y][x]
     * @throws IOException
     */
    private void saveToFile(double[][] val) throws IOException
    {
        BufferedImage bmp = new BufferedImage(WIDTH, DEPTH, BufferedImage.TYPE_4BYTE_ABGR);
        double max, min, scale;
        max = Double.MIN_VALUE;
        min = Double.MAX_VALUE;

        for (int y = 0; y < DEPTH; y++)
        {
            for (int x = 0; x < WIDTH; x++)
            {
                if (min > val[y][x])
                {
                    min = val[y][x];
                }

                if (max < val[y][x])
                {
                    max = val[y][x];
                }
            }
        }

        scale = max - min;

        for (int y = 0; y < DEPTH; y++)
        {
            for (int x = 0; x < WIDTH; x++)
            {
                int rgb = (int)(((val[y][x] - min) / scale) * 255);

                Color color = new Color(rgb, rgb, rgb);
                //bmp.setRGB(x, YDIM - y - 1, color.getRGB()); // 뒤집혀 있음
                bmp.setRGB(x, y, color.getRGB());
            }
        }

        ImageIO.write(bmp, "png", new File("test.png"));
    }

    /**
     * 700 기압에서의 상당온위값
     * @param lat 위도
     * @param lon 경도
     * @return 상당온위값
     */
    public double getEq700Value(double lat, double lon)
    {
        PointF pointF;
        Lambert lambert;
        lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDAPS_SGL_EQVPTL);

        pointF = lambert.wgs84ToLambert(lat, lon);

        double[][] gg = getEqPotentialTempAt700();
        return gg[(int)pointF.getX()][(int)pointF.getY()];
    }

    /**
     * 850 기압에서의 상당온위값
     * @param lat 위도
     * @param lon 경도
     * @return 상당온위값
     */
    public double getEq850Value(double lat, double lon)
    {
        PointF pointF;
        Lambert lambert;
        lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDAPS_SGL_EQVPTL);

        pointF = lambert.wgs84ToLambert(lat, lon);

        double[][] gg = getEqPotentialTempAt850();
        return gg[(int)pointF.getX()][(int)pointF.getY()];
    }

    /**
     * 925 기압에서의 상당온위값
     * @param lat 위도
     * @param lon 경도
     * @return 상당온위값
     */
    public double getEq925Value(double lat, double lon)
    {
        Lambert lambert;
        lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDAPS_SGL_EQVPTL);

        PointF coord = lambert.wgs84ToLambert(lat, lon);

        double[][] gg = getEqPotentialTempAt925();
        return gg[(int)coord.getX()][(int)coord.getY()];
    }

    /**
     * 700 기압에서의 상당온위값 계산
     * @return 전체 지도에 대한 상당온위값 (x, y 격자 배열)
     */
    private double[][] getEqPotentialTempAt700()
    {
        double[][] T700 = new double[DEPTH][WIDTH];
        double[][] D700 = new double[DEPTH][WIDTH];

        for (int y = 0; y < DEPTH; y++)
        {
            for (int x = 0; x < WIDTH; x++)
            {
                T700[y][x] = _lstLayers.get(10)[y][x];
                D700[y][x] = Math.max(_lstLayers.get(11)[y][x], 0.0);
            }
        }

        return _calc_epotemp(T700, D700, 700.0);
    }

    /**
     * 850 기압에서의 상당온위값 계산
     * @return 전체 지도에 대한 상당온위값 (x, y 격자 배열)
     */
    private double[][] getEqPotentialTempAt850()
    {
        double[][] T700 = new double[DEPTH][WIDTH];
        double[][] D700 = new double[DEPTH][WIDTH];

        for (int y = 0; y < DEPTH; y++)
        {
            for (int x = 0; x < WIDTH; x++)
            {
                T700[y][x] = _lstLayers.get(5)[y][x];
                D700[y][x] = Math.max(_lstLayers.get(6)[y][x], 0.0);
            }
        }

        return _calc_epotemp(T700, D700, 850.0);
    }

    /**
     * 925 기압에서의 상당온위값 계산
     * @return 전체 지도에 대한 상당온위값 (x, y 격자 배열)
     */
    private double[][] getEqPotentialTempAt925()
    {
        double[][] T700 = new double[DEPTH][WIDTH];
        double[][] D700 = new double[DEPTH][WIDTH];

        for (int y = 0; y < DEPTH; y++)
        {
            for (int x = 0; x < WIDTH; x++)
            {
                T700[y][x] = _lstLayers.get(29)[y][x];
                D700[y][x] = Math.max(_lstLayers.get(30)[y][x], 0.0);
            }
        }

        return _calc_epotemp(T700, D700, 925.0);
    }

    /**
     * 상당온위값 계산 (parameter 설명 추후 수정 필요)
     * @param T700 온도에 대한 격자값
     * @param D700 상위 layer에 대한 격자 값
     * @param pres 기압값
     * @return 상당온위 (온도값)
     */
    private double[][] _calc_epotemp(double[][] T700, double[][] D700, double pres)
    {
        double[][] eq = new double[DEPTH][WIDTH];
        double e6 = Math.pow(10, 6);

        for (int y = 0; y < DEPTH; y++)
        {
            for (int x = 0; x < WIDTH; x++)
            {
                double TD70 = T700[y][x] - D700[y][x];
                double pt = (T700[y][x] + 273.15) * Math.pow((1000.0 / pres), 0.285857);
                double evap = 6.11 * Math.exp((17.269 * (TD70 + 273.15) - 4717.3) / ((TD70 + 273.15) - 35.86));
                double rmix = (0.622 * evap) / (pres - evap);
                eq[y][x] = pt * Math.exp((2.5 * e6 * rmix) / (1004 * (T700[y][x] + 273.15)));
            }
        }

        return eq;
    }
}