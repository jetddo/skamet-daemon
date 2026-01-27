package kama.daemon.model.prediction.adopt.FOG_LR;

import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DateFormatter;
import org.apache.commons.lang3.time.DateUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author chlee
 * Created on 2017-01-25.
 */
public class HeightTempLoader
{
    //
    // read_lfog.ncl 로부터 변환한 코드
    // 추가 참고 코드: 1st_fogfcst.ncl, 2nd_fogfcst_tz.ncl, 3rd_fogfcst_z0.ncl
    //
    int nz_pres_floor = 24, nt_time_hr = 36; // nz = 24, nt = 36

    double uwind[][], vwind[][], tempr[][], psurf[], qvapo[][], lwatr[][];
    double u_obs[][], v_obs[][], t_obs[][], w_obs[][], z_obs[][], q_obs[][], l_obs[][];
    double mslpo[], td2mo[], rh2mo[], trefa[], visib[], cldfr[][], prcpt[], relhu[][], tdewp[][];

    double min_mslpo, max_mslpo;

    private int _stationID;
    private Date _timeUTC, _timeKST;

    public class HeightTempInfo
    {
        public int station_id;
        public Date predicted_time; // KST 기준
        public Date model_produced_time; // UTC 기준
        public int floor;
        public double u_wind;
        public double v_wind;
        public double temperature;
        public double dew_point;
        public double rel_humidity;
        public double pressure;
    }

    public HeightTempLoader(File file, Date timeUTC, Date timeKST, int stationID) throws IOException, ParseException
    {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
                FileChannel channel = raf.getChannel())
        {
            long fileSize = raf.length();

            _stationID = stationID;
            _timeUTC = timeUTC;
            _timeKST = timeKST;

            ByteBuffer buffer = ByteBuffer.allocateDirect((int)fileSize).order(ByteOrder.BIG_ENDIAN);
            channel.read(buffer);
            buffer.clear();

            FloatBuffer fbuf = buffer.asFloatBuffer();
            fbuf.clear();

            uwind = new double[nt_time_hr][];
            vwind = new double[nt_time_hr][];
            tempr = new double[nt_time_hr][];
            psurf = new double[nt_time_hr];
            qvapo = new double[nt_time_hr][];
            lwatr = new double[nt_time_hr][];
            u_obs = new double[nt_time_hr][];
            v_obs = new double[nt_time_hr][];
            t_obs = new double[nt_time_hr][];
            w_obs = new double[nt_time_hr][];
            z_obs = new double[nt_time_hr][];
            q_obs = new double[nt_time_hr][];
            l_obs = new double[nt_time_hr][];
            mslpo = new double[nt_time_hr];
            td2mo = new double[nt_time_hr];
            rh2mo = new double[nt_time_hr];
            trefa = new double[nt_time_hr];
            visib = new double[nt_time_hr];
            cldfr = new double[nt_time_hr][];
            prcpt = new double[nt_time_hr];
            relhu = new double[nt_time_hr][];
            tdewp = new double[nt_time_hr][];

            for (int y = 0; y < nt_time_hr; y++)
            {
                readArrayElements(uwind, y, fbuf);
                readArrayElements(vwind, y, fbuf);
                readArrayElements(tempr, y, fbuf);
                readArrayElements(psurf, y, fbuf);
                readArrayElements(qvapo, y, fbuf);
                readArrayElements(lwatr, y, fbuf);
                readArrayElements(u_obs, y, fbuf);
                readArrayElements(v_obs, y, fbuf);
                readArrayElements(t_obs, y, fbuf);
                readArrayElements(w_obs, y, fbuf);
                readArrayElements(z_obs, y, fbuf);
                readArrayElements(q_obs, y, fbuf);
                readArrayElements(l_obs, y, fbuf);
                readArrayElements(mslpo, y, fbuf);
                readArrayElements(td2mo, y, fbuf);
                readArrayElements(rh2mo, y, fbuf);
                readArrayElements(trefa, y, fbuf);
                readArrayElements(visib, y, fbuf);
                readArrayElements(cldfr, y, fbuf);
                readArrayElements(prcpt, y, fbuf);
                readArrayElements(relhu, y, fbuf);
                readArrayElements(tdewp, y, fbuf);
            }

            min_mslpo = Double.MAX_VALUE;
            max_mslpo = Double.MIN_VALUE;

            for (double pres : mslpo)
            {
                if (pres < min_mslpo)
                {
                    min_mslpo = pres;
                }

                if (pres > max_mslpo)
                {
                    max_mslpo = pres;
                }
            }
        }
    }

    public List<HeightTempInfo> retrieveData()
    {
        List<HeightTempInfo> htInfoList;
        HeightTempInfo htInfo;

        htInfoList = new ArrayList<>();

        for (int time = 0; time < nt_time_hr; time++)
        {
            for (int floor = 0; floor < nz_pres_floor; floor++)
            {
                htInfo = new HeightTempInfo();

                htInfo.station_id = _stationID;
                htInfo.floor = floor;
                htInfo.predicted_time = DateUtils.addHours((Date) _timeKST, time); // KST + 예측 hours
                htInfo.model_produced_time = _timeUTC;
                htInfo.u_wind = uwind[time][floor];
                htInfo.v_wind = vwind[time][floor];
                htInfo.temperature = tempr[time][floor] - 273.15;
                htInfo.dew_point = tdewp[time][floor] - 273.15;
                htInfo.rel_humidity = (relhu[time][floor] * 100) % 100;
                htInfo.pressure = ((mslpo[time] - min_mslpo) / (max_mslpo - min_mslpo)) * 20.0 + 10.0;

                htInfoList.add(htInfo);
            }
        }

        return htInfoList;
    }

    private void readArrayElements(double[] array, int y, FloatBuffer fbuf)
    {
        array[y] = fbuf.get();
    }

    private void readArrayElements(double[][] array, int y, FloatBuffer fbuf)
    {
        array[y] = new double[nz_pres_floor];

        for (int x = 0; x < nz_pres_floor; x++)
        {
            array[y][x] = fbuf.get();
        }
    }
}