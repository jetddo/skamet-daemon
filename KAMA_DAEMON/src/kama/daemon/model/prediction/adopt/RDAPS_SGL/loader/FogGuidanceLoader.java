package kama.daemon.model.prediction.adopt.RDAPS_SGL.loader;

import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.PointF;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Sangjin
 * Created on 11/22/2016.
 */
public class FogGuidanceLoader
{
    // ************************** 구름변수: 안개가이던스 ***********************
    private String fname;

    public FogGuidanceLoader(String _fname) throws IOException
    {
        fname = _fname;
        Parse();
    }

    private int id = 491, jd = 419, nt = 12;
    private double[][][] data = new double[id][jd][nt];
    private double[][] _visb_info_calculated;
    private Lambert.lamc_parameter lambert_param;

    private void Parse() throws IOException
    {
        lambert_param = new Lambert.lamc_parameter();

        lambert_param.Re = 6371.00877; // Earth radius (km) [사용할 지구 반경]
        lambert_param.grid = 12; // for 6 km [격자간격] // LC 1.2 // 6
        lambert_param.slat1 = 30.0; // Standard latitude 1 [표준 위도1] (최하단 위도)
        lambert_param.slat2 = 60.0; // Standard latitude 2 [표준 위도2] // TRUE_LAT_NORTH (최상단 위도)
        lambert_param.center_lat = 38.0; // latitude of known point in map (degree) [기준점의 위도]
        lambert_param.center_lon = 126.0; // longitude of known point in map (degree) [기준점의 경도]
        lambert_param.xo = 245.0; // for 6 km [기준점의 X좌표] // 262 (평면좌표의 x 길이를 반으로 나눈 값)
        lambert_param.yo = 210.0; // for 6 km [기준점의 Y좌표] // 419 (평면좌표의 y 길이를 반으로 나눈 값)

        try (RandomAccessFile file = new RandomAccessFile(fname, "r");
             FileChannel channel = file.getChannel())
        {
            long nsize = file.length();

            ByteBuffer buffer = ByteBuffer.allocateDirect((int) nsize).order(ByteOrder.BIG_ENDIAN);
            channel.read(buffer);
            buffer.clear();

            FloatBuffer dbuf = buffer.asFloatBuffer();
            dbuf.clear();
            //usst = data;

            for (int n = 0; n < nt; n++)
            {
                for (int j = 0; j < jd; j++)
                {
                    for (int i = 0; i < id; i++)
                    {
                        data[i][j][n] = dbuf.get();
                    }
                }
            }
        }

        _visb_info_calculated = retrieveVisibilityInfo();
    }

    // 뽑을 수 있는 정보들 (3차원 인덱스값)
    private int IDX_THETA_AFTER_TIMESTEP = 0;   // Potential Temperature
    private int IDX_SPECIFIC_HUMIDITY = 1;  // Specific Humidity
    private int IDX_QCF_AFTER_TIMESTEP = 2; // Cloud Ice
    private int IDX_QCL_AFTER_TIMESTEP = 3; // Cloud Liquid Water
    private int IDX_EXNER_PRESSURE_RHO_AFTER_TIMESTEP = 4;  // Exner Pressure
    private int IDX_RAIN_AFTER_TIMESTEP = 5;    // Rain Water Mixing Ratio

    // 안나올시에는 위와 같이 표출해서 테스트 해보면 됨.
    private double[][] aa()
    {
        double[][] temp = new double[id][jd];

        for (int j = 0; j < jd; j++)
        {
            for (int i = 0; i < id; i++)
            {
                temp[i][j] = data[i][j][2];
            }
        }

        return temp;
    }

    public double getVisbValue(double lat, double lon)
    {
        PointF pointF;
        Lambert lambert;
        lambert = new Lambert(lambert_param);

        pointF = lambert.wgs84ToLambert(lat, lon);

        return _visb_info_calculated[(int)pointF.getX()][(int)pointF.getY()];
    }

    private double[][] retrieveVisibilityInfo()
    {
        double rhowat = 1000.;
        double rhoice = 917.;
        double rgas = 287.04;
        double cp = 1005.0;
        double cpr = cp / rgas;

        double celkel = 273.15;
        double tice = celkel - 10.;
        double coeflc = 144.7;
        double coeflp = 2.24;
        double coeffc = 327.8;
        double coeffp = 10.36;
        double exponlc = 0.88;
        double exponlp = 0.7500;
        double exponfc = 1.0000;
        double exponfp = 0.7776;

        double const1 = -Math.log(0.02);
        double[][] vis = new double[id][jd];

        for (int j = 0; j < jd; j++)
        {
            for (int i = 0; i < id; i++)
            {
                double enpm = (data[i][j][IDX_EXNER_PRESSURE_RHO_AFTER_TIMESTEP * 2] + data[i][j][IDX_EXNER_PRESSURE_RHO_AFTER_TIMESTEP * 2 + 1]) * 0.5;
                double tk = data[i][j][IDX_THETA_AFTER_TIMESTEP * 2] / enpm; // Temperature
                double p = 100000.0 * Math.pow(enpm, cpr);
                double rhoair = p / (rgas * tk * (1.0 + 0.61 * data[i][j][IDX_SPECIFIC_HUMIDITY * 2]));
                double vovermd = (1.0 + data[i][j][IDX_SPECIFIC_HUMIDITY * 2]) / rhoair
                        + (data[i][j][IDX_QCL_AFTER_TIMESTEP * 2] + data[i][j][IDX_RAIN_AFTER_TIMESTEP * 2]) / rhowat
                        + (data[i][j][IDX_QCF_AFTER_TIMESTEP * 2] * 0.1) / rhoice;

                double conclc = data[i][j][IDX_QCL_AFTER_TIMESTEP * 2] / vovermd * 1000.;
                double conclp = data[i][j][IDX_RAIN_AFTER_TIMESTEP * 2] / vovermd * 1000.;
                double concfc = data[i][j][IDX_QCF_AFTER_TIMESTEP * 2] * 0.1 / vovermd * 1000.;
                double concfp = 0.0;

                double beta = coeffc * Math.pow(concfc, exponfc) + coeffp * Math.pow(concfp, exponfp)
                        + coeflc * Math.pow(conclc, exponlc) + coeflp * Math.pow(conclp, exponlp) + 1.e-10;


                //const1=-log(.02) ==> 정의되어있음
                // lfog_visb.2017010100_000 에서 데이터 읽어 이미지 표출해보면 됨.
                vis[i][j] = const1 / beta;

                if (vis[i][j] >= 24.135)
                {
                    vis[i][j] = 24.135;
                }
            }
        }

        return smth_9pntgrd(vis);
    }

    // 스무딩
    private double[][] smth_9pntgrd(double[][] data)
    {
        double sum = 0;
        int nc = 0;
        double[][] snew = null;
        double[][] temp = null;
        temp = data.clone();
        for (int k = 0; k < 1; k++)
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

                                double t = temp[ii][jj];
                                if (t != -999.0)
                                {
                                    nc += 1;
                                    sum += t;
                                }
                            }

                            if (nc > 0)
                            {
                                //System.out.println((float)(sum / nc));
                                snew[i][j] = (double) (sum / nc);
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
}