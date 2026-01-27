package kama.daemon.model.prediction.adopt.LDAPS_SGL.loader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;

import kama.daemon.common.util.DaemonSettings;

/**
 * @author Sangjin
 * Created on 11/17/2016.
 */
public class UFogLoader
{
    // 안개-시정
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

    private int ierrf = 6, lunit = 2, iwtype = 21, iwkid = 1;
    private int id = 603, jd = 797, ks = 2, nt = 1, lrwk = 30000, liwk = 30000;
    private int kxs = 1, kxe = id, kys = 1, kye = 797, itsm = 100, ism = 30;
    private int jmx = 200, ncfe = 8, ncle = 12, icx = 17;

    private String fname = null;

    private double[][] fvis = new double[id][jd];
    private double[][] frac = new double[id][jd];
    private double[][][] fvisg = new double[id][jd][nt];
    private double[][][] fracg = new double[id][jd][nt];
    private String[] cln = new String[jmx];
    private double[][] cle = new double[ncle][jmx];

    private double[][] plat = new double[id][jd];
    private double[][] plon = new double[id][jd];

    public UFogLoader(String _fname)
    {
        fname = _fname;
        loadConfig();
        Parse();
    }

    private void loadConfig()
    {
        try
        {
            try (BufferedReader in = new BufferedReader(new FileReader(String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/daba_ldps_conline.dat"))))
            {
                String s;

                int cnt = 0;
                int indx = 0;
                while ((s = in.readLine()) != null)
                {
                    if (cnt < 5)
                    {
                        cnt += 1;
                        continue;
                    }

                    if (s.trim().isEmpty() == true)
                    {
                        cnt += 1;
                        continue;
                    }

                    if (s.startsWith("======"))
                        break;

                    int pos = 0;
                    cln[indx] = s.substring(0, 5);
                    pos += 5;
                    cle[0][indx] = Double.parseDouble(s.substring(pos, pos + 3));
                    pos += 3;
                    cle[1][indx] = Double.parseDouble(s.substring(pos, pos + 3));
                    pos += 3;
                    cle[2][indx] = Double.parseDouble(s.substring(pos, pos + 4));
                    pos += 4;
                    cle[3][indx] = Double.parseDouble(s.substring(pos, pos + 5));
                    pos += 5;
                    cle[4][indx] = Double.parseDouble(s.substring(pos, pos + 10));
                    pos += 10;
                    cle[5][indx] = Double.parseDouble(s.substring(pos, pos + 10));
                    pos += 10;
                    cle[6][indx] = Double.parseDouble(s.substring(pos, pos + 10));
                    pos += 10;
                    cle[7][indx] = Double.parseDouble(s.substring(pos, pos + 5));
                    pos += 5;
                    cle[8][indx] = Double.parseDouble(s.substring(pos, pos + 5));
                    pos += 5;
                    cle[9][indx] = Double.parseDouble(s.substring(pos, pos + 5));
                    pos += 5;
                    cle[10][indx] = Double.parseDouble(s.substring(pos, pos + 5));
                    pos += 5;
                    cle[11][indx] = Double.parseDouble(s.substring(pos, pos + 5));
                    pos += 5;

                    if (cnt > 90)
                    {
                        int a = 100;
                    }

                    indx += 1;
                    cnt += 1;
                }
            }

            for (int j = 0; j < jd; j++)
            {
                for (int i = 0; i < id; i++)
                {
                    InnerDouble lat = new InnerDouble();
                    InnerDouble lon = new InnerDouble();
                    InnerDouble X = new InnerDouble(i);
                    InnerDouble Y = new InnerDouble(j);
                    lamcproj(lat, lon, X, Y, 1);
                    plat[i][j] = lat.getValue();
                    plon[i][j] = lon.getValue();
                }
            }
        }
        catch (IOException e)
        {
            System.err.println(e);
            System.exit(-1);
        }
    }

    private double fcut = 50.0;

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

            for (int n = 0; n < nt; n++)
            {
                float[][] temp = new float[id][jd];

                for (int j = 0; j < jd; j++)
                {
                    for (int i = 0; i < id; i++)
                    {
                        temp[i][j] = (float) Math.min(dbuf.get() * 0.001, fcut);
                    }
                }

//                smth_9pntgrd(temp);

                for (int j = 0; j < jd; j++)
                {
                    for (int i = 0; i < id; i++)
                    {
                        fvisg[i][j][n] = temp[i][j];
                    }
                }

                temp = null;
                temp = new float[id][jd];

                for (int j = 0; j < jd; j++)
                {
                    for (int i = 0; i < id; i++)
                    {
                        temp[i][j] = (float) (dbuf.get() * 100.0);
                    }
                }

//                smth_9pntgrd(temp);

                for (int j = 0; j < jd; j++)
                {
                    for (int i = 0; i < id; i++)
                    {
                        fracg[i][j][n] = temp[i][j];
                    }
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

    private float[][] smth_9pntgrd(float[][] data)
    {
        double sum = 0;
        int nc = 0;
        float[][] snew = null;
        float[][] temp = null;
        temp = data.clone();
        for (int k = 0; k < itsm; k++)
        {
            snew = data.clone();

            for (int j = 0; j < jd; j++)
            {
                for (int i = 0; i < id; i++)
                {
                    if (k < 2 || temp[i][j] != -999.0)
                    {
                        nc = 0;
                        sum = 0;

                        for (int jj = j - 1; jj <= j + 1; jj++)
                        {
                            for (int ii = i - 1; ii <= i + 1; ii++)
                            {
                                if (jj < 0 || jj >= jd) continue;
                                if (ii < 0 || ii >= id) continue;

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

    // 지정된 위치에서 가장 가까운 격자의 정보를 시간 배열로 돌려준다.
    public double[] getVisGValue(double lat, double lon)
    {
        if (lat < plat[0][0] || lat > plat[id - 1][jd - 1])
            return null;
        if (lon < plon[0][0] || lat > plon[id - 1][jd - 1])
            return null;

        double minval = 99999999999.0;
        int i_index = -1;
        int j_index = -1;

        // (ppfl_brny script에서 변경) 현 격자 모델을 Arakawa C라고 가정을 한다.
        // 그럼 격자의 중심점이 인덱스의 중심이기 때문에, plat, plon은 격자 중심점의 위치를 의미한다.
        // 여기서 특정 경위도 값에 대해서 어떤 그리드가 가장 가까운지를 계산하는 방법은
        // euclidean distance를 계산해서 이것이 가장 작을때의 값으로 결과를 취한다.
        // 물론 곡률을 고려하면 거리는 euclidean distance로 구하면 안된다.
        for (int j = 0; j < jd; j++)
        {
            for (int i = 0; i < id; i++)
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

        double[] ret = new double[nt];
        for (int i = 0; i < nt; i++)
        {
            ret[i] = fvisg[i_index][j_index][i];
        }

        return ret;
    }

    // 지정된 위치에서 가장 가까운 격자의 정보를 시간 배열로 돌려준다.
    public double[] getFracGValue(double lat, double lon)
    {
        if (lat < plat[0][0] || lat > plat[id - 1][jd - 1])
            return null;
        if (lon < plon[0][0] || lat > plon[id - 1][jd - 1])
            return null;

        double minval = 99999999999.0;
        int i_index = -1;
        int j_index = -1;

        // (ppfl_brny script에서 변경) 현 격자 모델을 Arakawa C라고 가정을 한다.
        // 그럼 격자의 중심점이 인덱스의 중심이기 때문에, plat, plon은 격자 중심점의 위치를 의미한다.
        // 여기서 특정 경위도 값에 대해서 어떤 그리드가 가장 가까운지를 계산하는 방법은
        // euclidean distance를 계산해서 이것이 가장 작을때의 값으로 결과를 취한다.
        // 물론 곡률을 고려하면 거리는 euclidean distance로 구하면 안된다.
        for (int j = 0; j < jd; j++)
        {
            for (int i = 0; i < id; i++)
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

        double[] ret = new double[nt];
        for (int i = 0; i < nt; i++)
        {
            ret[i] = fracg[i_index][j_index][i];
        }

        return ret;
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