package kama.daemon.model.prediction.adopt.RDAPS_ISOB.loader;

import kama.daemon.common.util.DaemonSettings;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * @author Sangjin
 * Created on 11/16/2016.
 */
public class VerticalVisibilityLoader
{
    // ************************** 연직시계열 **********************
    // NT : time
    // JN : pressure level (1000: 0)
    // MCT : maximum number of cities
    private int JL = 16, JN = 24, MCT = 100, NMCT = 7, NR = 4, NT = 30;
    private int LY = 6, NMAX = 100;
    private int IERRF = 6, LUNIT = 2, IWTYPE = 20, IWKID = 1, IPG = 3;
    private int LRWK = 30000, LIWK = 30000, ICNT_SMOTH = 1;

    private String CITY_INFO_FILE = String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/r512_meteocity_korea.dat");
    private List<CityInfo> cities;

    private double[][][] puw = new double[JN][NT][MCT];
    private double[][][] pvw = new double[JN][NT][MCT];
    private double[][][] pte = new double[JN][NT][MCT];
    private double[][][] prh = new double[JN][NT][MCT];
    private double[][][] pcw = new double[JN][NT][MCT];
    private double[][][] pci = new double[JN][NT][MCT];
    private double[][] suw = new double[NT][MCT];
    private double[][] svw = new double[NT][MCT];
    private double[][] sgw = new double[NT][MCT];
    private double[][] ste = new double[NT][MCT];
    private double[][] srh = new double[NT][MCT];
    private double[][] sps = new double[NT][MCT];
    private double[][] srt = new double[NT][MCT];
    private double[][] ssl = new double[NT][MCT];
    private double[][] ssc = new double[NT][MCT];
    private double[][] srl = new double[NT][MCT];
    private double[][] src = new double[NT][MCT];

    private double[][] tpw = new double[NT][MCT];
    private double[][] dcp = new double[NT][MCT];
    private double ccut = 0.000001;

    public double[] IPR = {1000, 975, 950, 925, 900, 875, 850, 800, 750, 700,
            650, 600, 550, 500, 450, 400, 350, 300, 250, 200,
            150, 100, 70, 60};
    private boolean[] IFG = {true, false, true, false, true, false, true, true, true,
            true, true, true, true, true, true, true, true, false,
            false, false, false, false};

    private String fname = null;

    public VerticalVisibilityLoader(String _dataFilePath) throws IOException
    {
        cities = CityInfo.loadCityInfo(CITY_INFO_FILE);
        fname = _dataFilePath;

        _parseData();
    }

    public static int floorIndexToHectoPascal(int index)
    {
        int[] pressureList = { 1000, 975, 950, 925, 900, 875, 850, 800, 750, 700, 650, 600, 550, 500, 450, 400, 350, 300, 250, 200, 150, 100, 70, 50 };

        return pressureList[index];
    }

    public static int floorIndexToAltitudeMeter(int index)
    {
        int[] altitudeMeters = { 111, 323, 540, 762, 988, 1267, 1457, 1949, 2644, 3012, 3591, 4206, 4865, 5574, 6343, 7185, 8117, 9164, 10363, 11774, 13508, 15797, 17669, 19322 };

        return altitudeMeters[index];
    }

    private int getIndex(int stnid)
    {
        for (int i = 0; i < cities.size(); i++)
        {
            CityInfo info = cities.get(i);

            if (info.stnid == stnid)
            {
                return i;
            }
        }

        return -1;
    }

    public double[][] getTemperatureListByPresAndTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        double[][] temp = new double[JN][NT]; // NT : 시간
        for (int i = 0; i < JN; i++)
        {
            for (int j = 0; j < NT; j++)
            {
                temp[i][j] = pte[i][j][index];
            }
        }

        return temp;
    }

    public double[][] getWindUListByPresAndTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        double[][] temp = new double[JN][NT];
        for (int i = 0; i < JN; i++)
        {
            for (int j = 0; j < NT; j++)
            {
                temp[i][j] = puw[i][j][index];
            }
        }

        return temp;
    }

    public double[][] getWindVListByPresAndTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        double[][] temp = new double[JN][NT];
        for (int i = 0; i < JN; i++)
        {
            for (int j = 0; j < NT; j++)
            {
                temp[i][j] = pvw[i][j][index];
            }
        }

        return temp;
    }

    public double[][] getCloudWaterInfoListByPresAndTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        double[][] temp = new double[JN][NT];
        for (int i = 0; i < JN; i++)
        {
            for (int j = 0; j < NT; j++)
            {
                temp[i][j] = pcw[i][j][index];
            }
        }

        return temp;
    }

    public double[][] getCloudIceInfoListByPresAndTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        double[][] temp = new double[JN][NT];
        for (int i = 0; i < JN; i++)
        {
            for (int j = 0; j < NT; j++)
            {
                temp[i][j] = pci[i][j][index];
            }
        }

        return temp;
    }

    public double[] getSurfacePressureListByTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        double[] temp = new double[NT];
        for (int i = 0; i < NT; i++)
        {
            temp[i] = sps[i][index];
        }

        return temp;
    }

    public double[] getRelativeHumidityListByTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        double[] temp = new double[NT];
        for (int i = 0; i < NT; i++)
        {
            temp[i] = Math.min(srh[i][index], 100.);
        }

        return temp;
    }

    // m/s
    public double[] getWindUSpeedListByTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        // airport일 경우는 1.9425를 곱해주어야 함 : m/s->knot
        double[] temp = new double[NT];
        for (int i = 0; i < NT; i++)
        {
            temp[i] = suw[i][index];
        }

        return temp;
    }

    // m/s
    public double[] getWindVSpeedListByTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        // airport일 경우는 1.9425를 곱해주어야 함 : m/s->knot
        double[] temp = new double[NT];
        for (int i = 0; i < NT; i++)
        {
            temp[i] = svw[i][index];
        }

        return temp;
    }

    //??
    public double[] getWindGSpeedListByTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        // airport일 경우는 1.9425를 곱해주어야 함 : m/s->knot
        double[] temp = new double[NT];
        for (int i = 0; i < NT; i++)
        {
            temp[i] = sgw[i][index];
        }

        return temp;
    }

    // sqrt(u*u+v*v) : wind speed
    public double[] getWindSpeedListByTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        // airport일 경우는 1.9425를 곱해주어야 함 : knot
        double[] temp = new double[NT];
        for (int i = 0; i < NT; i++)
        {
            temp[i] = Math.sqrt(Math.pow(suw[i][index] * 1.9425, 2.) + Math.pow(svw[i][index] * 1.9425, 2.));
        }

        return temp;
    }

    public double[] getRainInfoListByTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        double temp[] = new double[NT];
        for (int i = 0; i < NT; i++)
        {
            temp[i] = Math.max(srt[i][index], 0.);
        }

        return temp;
    }

    public double[] getLowestSigmaHTemperatureListByTime(int stnid)
    {
        int index = getIndex(stnid);

        if (index == -1)
        {
            return null;
        }

        double temp[] = new double[NT];
        for (int i = 0; i < NT; i++)
        {
            temp[i] = ste[i][index];
        }

        return temp;
    }

    /**
     * 데이터 파일로부터 데이터 전체를 미리 읽어들임
     * @throws IOException
     */
    private void _parseData() throws IOException
    {
        try (RandomAccessFile file = new RandomAccessFile(fname, "r");
             FileChannel channel = file.getChannel())
        {
            long nsize = file.length();

            ByteBuffer buffer = ByteBuffer.allocateDirect((int) nsize).order(ByteOrder.BIG_ENDIAN);
            channel.read(buffer);
            buffer.clear();

            FloatBuffer fbuf = buffer.asFloatBuffer();
            fbuf.clear();

            for (int n = 0; n < NT; n++)
            {
                for (int j = 0; j < JN; j++)
                {
                    _parseDataInternal3DArray(fbuf, j, n);
                }

                _parseDataInternal2DArray(fbuf, n);
            }

            //usst = data;
        }
    }

    /**
     * 2차원 배열값들에 대한 데이터 파싱
     * @param fbuf
     * @param n
     */
    private void _parseDataInternal2DArray(FloatBuffer fbuf, int n)
    {
        for (int m = 0; m < cities.size(); m++)
        {
            suw[n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        for (int m = 0; m < cities.size(); m++)
        {
            svw[n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        for (int m = 0; m < cities.size(); m++)
        {
            sgw[n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        // 온도
        for (int m = 0; m < cities.size(); m++)
        {
            ste[n][m] = fbuf.get() - 273.15;
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        for (int m = 0; m < cities.size(); m++)
        {
            srh[n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        for (int m = 0; m < cities.size(); m++)
        {
            sps[n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        for (int m = 0; m < cities.size(); m++)
        {
            srt[n][m] = fbuf.get();

            if (srt[n][m] <= 0.)
            {
                srt[n][m] = 0.;
            }
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        for (int m = 0; m < cities.size(); m++)
        {
            ssl[n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        for (int m = 0; m < cities.size(); m++)
        {
            ssc[n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        for (int m = 0; m < cities.size(); m++)
        {
            srl[n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        for (int m = 0; m < cities.size(); m++)
        {
            src[n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        for (int m = 0; m < cities.size(); m++)
        {
            tpw[n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        for (int m = 0; m < cities.size(); m++)
        {
            dcp[n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();
    }

    /**
     * 3차원 배열값들에 대한 데이터 파싱
     * @param fbuf
     * @param j
     * @param n
     */
    private void _parseDataInternal3DArray(FloatBuffer fbuf, int j, int n)
    {
        // wind u vector
        for (int m = 0; m < cities.size(); m++)
        {
            puw[j][n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        // wind v vector
        for (int m = 0; m < cities.size(); m++)
        {
            pvw[j][n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        // temperature
        for (int m = 0; m < cities.size(); m++)
        {
            pte[j][n][m] = fbuf.get() - 273.15;
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        for (int m = 0; m < cities.size(); m++)
        {
            prh[j][n][m] = fbuf.get();
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        // cloud water
        for (int m = 0; m < cities.size(); m++)
        {
            pcw[j][n][m] = fbuf.get();

            if (pcw[j][n][m] > ccut)
            {
                pcw[j][n][m] = 150.;
            }
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();

        // cloud ice
        for (int m = 0; m < cities.size(); m++)
        {
            pci[j][n][m] = fbuf.get();

            if (pci[j][n][m] > ccut)
            {
                pci[j][n][m] = 150.;
            }
        }

        // 필요없는 헤더 8 바이트 날려버림
        fbuf.get();
        fbuf.get();
    }
}