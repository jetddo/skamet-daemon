package kama.daemon.model.prediction.adopt.LDAPS_SGL.loader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;

/**
 * @author sangjinsim
 * Created on 16/11/2016.
 */
public class FogHumidityLoader
{
    // ******************** 안개습도예상도 *****************
    // 최적화는 나중에 하는 걸루...
    private class InnerDouble
    {
        private double val;

        public InnerDouble()
        {
            setValue(0.0);
        }

        public InnerDouble(double _val)
        {
            setValue(_val);
        }

        public double getValue()
        {
            return val;
        }

        public void setValue(double _val)
        {
            val = _val;
        }
    }

    private int IERRF = 6, LUNIT = 2, IWTYPE = 21, IWKID = 1, IPG = 4, NT = 1;
    private int ID = 603, JD = 797, LRWK = 30000, LIWK = 30000, ICNT_SMOTH = 1;
    private int NX = 4, NY = 4;

    private double[][][] sfcrh = null;
    private double[][] plat = new double[ID][JD];
    private double[][] plon = new double[ID][JD];

    private String fname;

    public FogHumidityLoader(String _fname, int _NT)
    {
        fname = _fname;
        NT = _NT;

        sfcrh = new double[ID][JD][NT]; // NT가 시간 JD 는 X, ID는 Y좌표 크기

        Parse();
    }

    private void Parse()
    {
        try
        {
            RandomAccessFile file = new RandomAccessFile(fname, "r");
            FileChannel channel = file.getChannel();

            long nsize = file.length();
            ByteBuffer buffer = ByteBuffer.allocateDirect((int) nsize).order(ByteOrder.BIG_ENDIAN);
            channel.read(buffer);
            buffer.clear();

            FloatBuffer dbuf = buffer.asFloatBuffer();
            dbuf.clear();

            for (int k = 0; k < NT; k++)
            {
                float[][] temp = new float[ID][JD];

                dbuf.get();
                dbuf.get();

                for (int j = 0; j < JD; j++)
                {
                    for (int i = 0; i < ID; i++)
                    {
                        temp[i][j] = dbuf.get();
                    }
                }

//                temp = smth_9pntgrd(temp);
                for (int j = 0; j < JD; j++)
                {
                    for (int i = 0; i < ID; i++)
                    {
                        sfcrh[i][j][k] = temp[i][j];
                    }
                }
            }

            for (int j = 0; j < JD; j++)
            {
                for (int i = 0; i < ID; i++)
                {
                    InnerDouble lat = new InnerDouble();
                    InnerDouble lon = new InnerDouble();
                    InnerDouble X = new InnerDouble(i);
                    InnerDouble Y = new InnerDouble(j);
                    lamcproj(lat, lon, X, Y, 1);
                    plat[i][j] = lat.getValue();
                    plon[i][j] = lon.getValue();

                    //System.out.println("(X, Y)" + "(" + X.getVisbValue() + "," + Y.getVisbValue() + ") = " + lat.getVisbValue() + "," + lon.getVisbValue());
                }
            }

            channel.close();
            file.close();
        }
        catch (IOException e)
        {
            System.err.println(e);
            System.exit(-1);
        }
    }

    // 지정된 위치에서 가장 가까운 격자의 정보를 시간 배열로 돌려준다.
    public double[] getValue(double lat, double lon)
    {
        if (NT == 0)
            return null;

        if (lat < plat[0][0] || lat > plat[ID - 1][JD - 1])
            return null;
        if (lon < plon[0][0] || lat > plon[ID - 1][JD - 1])
            return null;

        double minval = 99999999999.0;
        int i_index = -1;
        int j_index = -1;

        // (ppfl_brny script에서 변경) 현 격자 모델을 Arakawa C라고 가정을 한다.
        // 그럼 격자의 중심점이 인덱스의 중심이기 때문에, plat, plon은 격자 중심점의 위치를 의미한다.
        // 여기서 특정 경위도 값에 대해서 어떤 그리드가 가장 가까운지를 계산하는 방법은
        // euclidean distance를 계산해서 이것이 가장 작을때의 값으로 결과를 취한다.
        // 물론 곡률을 고려하면 거리는 euclidean distance로 구하면 안된다.
        for (int j = 0; j < JD; j++)
        {
            for (int i = 0; i < ID; i++)
            {
                double distance = Math.sqrt(Math.pow(plat[i][j] - lat, 2.) + Math.pow(plon[i][j] - lon, 2.));
                if (minval > distance)
                {
                    minval = distance;
                    i_index = i;
                    j_index = j;
                }
            }
        }

        double[] ret = new double[NT];
        for (int i = 0; i < NT; i++)
        {
            ret[i] = sfcrh[i_index][j_index][i];
        }

        return ret;
    }

    private double getRawValue(int x, int y)
    {
        if (sfcrh[0].length > x && sfcrh[0][x].length > y)
        {
            return sfcrh[y][x][0];
        }

        return -1;
    }

    private float[][] smth_9pntgrd(float[][] data)
    {
        double sum = 0;
        int nc = 0;
        float[][] snew = null;
        float[][] temp = null;
        temp = data.clone();
        for (int k = 0; k < ICNT_SMOTH; k++)
        {
            snew = data.clone();

            for (int j = 0; j < JD; j++)
            {
                for (int i = 0; i < ID; i++)
                {
                    if (k < 2 || temp[i][j] != -999.0)
                    {
                        nc = 0;
                        sum = 0;

                        for (int jj = j - 1; jj <= j + 1; jj++)
                        {
                            for (int ii = i - 1; ii <= i + 1; ii++)
                            {
                                if (jj < 0 || jj >= JD) continue;
                                if (ii < 0 || ii >= ID) continue;

                                float t = temp[ii][jj];
                                if (t != -999.0)
                                {
                                    nc += 1;
                                    sum += t;
                                }
                            }

                            if (nc > 0)
                            {
                                //System.out.println((float)(sum / nc));
                                snew[i][j] = (float) (sum / nc);
                                //System.exit(-1);
                            }
                        }
                    }
                }
            }
            temp = snew.clone();
        }

        return temp;
    }

    private boolean ifirst = false;

    private double PI = 3.14159265358979;
    private double DEGRAD = PI / 180.;
    private double RADDEG = 180. / PI;

    private double R = 6371.00877;  //           ! [km]     : Earth Radius
    private double SLAT1 = 30.0;    //             ! [degree] : Standard Latitude 1
    private double SLAT2 = 60.0;    //         ! [degree] : Standard Latitude 2
    private double OLAT = 38.0;    //         ! [degree] : Latitude of known point in map
    private double OLON = 126.0;   //         ! [degree] : Longitude of known point in map
    private double XO = 262.0;   //          ! [grid]   : X-coordinate of known point
    private double YO = 419.0;   //          ! [grid]   : Y-coordinate of known point
    private double DD = 1.5;   //         ! [km]     : Grid distance in map
    private double SN = 0.0;
    private double SF = 0.0;
    private double RO = 0.0;

    private void lamcproj(InnerDouble ALAT, InnerDouble ALON, InnerDouble X, InnerDouble Y, int n)
    {
        if (ifirst == false)
        {
            R = R / DD;
            SLAT1 = SLAT1 * DEGRAD;
            SLAT2 = SLAT2 * DEGRAD;
            OLAT = OLAT * DEGRAD;
            OLON = OLON * DEGRAD;

            if (SLAT1 == SLAT2 || Math.abs(SLAT1) >= PI * 0.5 || Math.abs(SLAT2) >= PI * 0.5)
            {
                System.err.println("Error");
                System.exit(-1);
            }

            SN = Math.tan(PI * 0.25 + SLAT2 * 0.5) / Math.tan(PI * 0.25 + SLAT1 * 0.5);
            SN = Math.log(Math.cos(SLAT1) / Math.cos(SLAT2)) / Math.log(SN);
            SF = Math.pow(Math.tan(PI * 0.25 + SLAT1 * 0.5), SN) * Math.cos(SLAT1) / SN;
            RO = 0.0;
            if (Math.abs(OLAT) > 89.9 * DEGRAD)
            {
                if (SN * OLAT < 0.0)
                {
                    System.err.println("Error");
                    System.exit(-1);
                }
            }
            else
            {
                RO = R * SF / Math.pow(Math.tan(PI * 0.25 + OLAT * 0.5), SN);
            }

            ifirst = true;
        }

        if (n == 0)
        {
            double RA = 0.0;
            if (Math.abs(ALAT.getValue()) > 89.9)
            {
                if (SN * ALAT.getValue() < 0.0)
                {
                    System.err.println("err");
                    System.exit(-1);
                }
                RA = 0.;
            }
            else
            {
                RA = R * SF / Math.pow(Math.tan(PI * 0.25 + ALAT.getValue() * DEGRAD * 0.5), SN);
            }
            double THETA = ALON.getValue() * DEGRAD - OLON;
            if (THETA > PI)
                THETA = THETA - 2. * PI;
            if (THETA < -PI)
                THETA = 2. * PI + THETA;
            THETA = SN * THETA;
            X.setValue(RA * Math.sin(THETA) + XO);
            Y.setValue(RO - RA * Math.cos(THETA) + YO);
        }
        else
        {
            double XN = X.getValue() - XO;
            double YN = RO - Y.getValue() + YO;
            double RA = Math.sqrt(XN * XN + YN * YN);
            if (SN < 0)
                RA = -RA;
            ALAT.setValue(Math.pow((R * SF / RA), (1. / SN)));
            ALAT.setValue(2. * Math.atan(ALAT.getValue()) - PI * 0.5);
            double THETA = 0.0;
            if (Math.abs(XN) <= 0.0)
            {
                THETA = 0.;
            }
            else
            {
                if (Math.abs(YN) <= 0.0)
                {
                    THETA = PI * 0.5;
                    if (XN < 0.0)
                        THETA = -THETA;
                }
                else
                {
                    THETA = Math.atan2(XN, YN);
                }
            }
            ALON.setValue(THETA / SN + OLON);
            ALAT.setValue(ALAT.getValue() * RADDEG);
            ALON.setValue(ALON.getValue() * RADDEG);
        }
    }
}