package kama.daemon.model.prediction.adopt.RDAPS_SGL.loader;

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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chlee
 * Created on 2017-01-09.
 */
public class EquivPotentialTempLoader
{
    private List<float[][]> _lstLayers;
    private float[][][] train;

    private final int WIDTH = 181, DEPTH = 161, HEIGHT = 34;
    private final int LAYER_COUNT = 34;

    private Lambert _lambert;

    /**
     * 클래스 생성자
     * @param filePath 데이터 파일 경로
     * @throws IOException
     */
    public EquivPotentialTempLoader(String filePath) throws IOException
    {
        Lambert.lamc_parameter param = new Lambert.lamc_parameter();
        param.xo = 91.0;
        param.yo = 81.0;
        param.grid = 30.0;
        _lambert = new Lambert(param);

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
        String ntimestr = Paths.get(filePath).getFileName().toString().split("_")[4].split("\\.")[0];
        int ntime = Integer.parseInt(ntimestr);
        int nt = ((int)(ntime/12)) * 4 + ((ntime%12)/3 + 1);

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
                dbuf.get();
                dbuf.get();

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

            train = new float[DEPTH][WIDTH][nt];
            for (int n = 0; n < nt; n++)
            {
                dbuf.get();
                dbuf.get();

                for (int y = 0; y < DEPTH; y++)
                {
                    for (int x = 0; x < WIDTH; x++)
                    {
                        train[y][x][n] = dbuf.get();
                    }
                }
            }
        }
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
        PointF coord;
        coord = _lambert.wgs84ToLambert(lat, lon);

        double[][] gg = getEqPotentialTempAt700();
        return gg[(int)coord.getX()][(int)coord.getY()];
    }

    /**
     * 850 기압에서의 상당온위값
     * @param lat 위도
     * @param lon 경도
     * @return 상당온위값
     */
    public double getEq850Value(double lat, double lon)
    {
        PointF coord = _lambert.wgs84ToLambert(lat, lon);

        double[][] gg = getEqPotentialTempAt850();
        return gg[(int)coord.getX()][(int)coord.getY()];
    }

    /**
     * 925 기압에서의 상당온위값
     * @param lat 위도
     * @param lon 경도
     * @return 상당온위값
     */
    public double getEq925Value(double lat, double lon)
    {
        PointF coord = _lambert.wgs84ToLambert(lat, lon);

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
        double[][] T850 = new double[DEPTH][WIDTH];
        double[][] D850 = new double[DEPTH][WIDTH];

        for (int y = 0; y < DEPTH; y++)
        {
            for (int x = 0; x < WIDTH; x++)
            {
                T850[y][x] = _lstLayers.get(5)[y][x];
                D850[y][x] = Math.max(_lstLayers.get(6)[y][x], 0.0);
            }
        }

        return _calc_epotemp(T850, D850, 850.0);
    }

    /**
     * 925 기압에서의 상당온위값 계산
     * @return 전체 지도에 대한 상당온위값 (x, y 격자 배열)
     */
    private double[][] getEqPotentialTempAt925()
    {
        double[][] T925 = new double[DEPTH][WIDTH];
        double[][] D925 = new double[DEPTH][WIDTH];

        for (int y = 0; y < DEPTH; y++)
        {
            for (int x = 0; x < WIDTH; x++)
            {
                T925[y][x] = _lstLayers.get(29)[y][x];
                D925[y][x] = Math.max(_lstLayers.get(30)[y][x], 0.0);
            }
        }

        return _calc_epotemp(T925, D925, 925.0);
    }

    /**
     * 상당온위값 계산 (parameter 설명 추후 수정 필요)
     * @param Tval 온도에 대한 격자값
     * @param Dval 상위 layer에 대한 격자 값
     * @param pres 기압값
     * @return 상당온위 (온도값)
     */
    private double[][] _calc_epotemp(double[][] Tval, double[][] Dval, double pres)
    {
        double[][] eq = new double[DEPTH][WIDTH];
        double e6 = Math.pow(10, 6);

        for (int y = 0; y < DEPTH; y++)
        {
            for (int x = 0; x < WIDTH; x++)
            {
                double TD70 = Tval[y][x] - Dval[y][x];
                double pt = (Tval[y][x] + 273.15) * Math.pow((1000.0 / pres), 0.285857);
                double evap = 6.11 * Math.exp((17.269 * (TD70 + 273.15) - 4717.3) / ((TD70 + 273.15) - 35.86));
                double rmix = (0.622 * evap) / (pres - evap);
                eq[y][x] = pt * Math.exp((2.5 * e6 * rmix) / (1004 * (Tval[y][x] + 273.15)));
            }
        }

        return eq;
    }
}