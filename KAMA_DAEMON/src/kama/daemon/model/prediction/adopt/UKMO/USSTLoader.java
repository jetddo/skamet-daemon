package kama.daemon.model.prediction.adopt.UKMO;

import kama.daemon.common.util.DaemonException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Sangjin
 * Created on 11/9/2016.
 */
public class USSTLoader
{
    private float[] usst = null;

    private int mt_width = 7200;
    private int mt_height = 3600;
    private int icnt_smoth = 50;
    private float bval = (float) 10000.0;
    private float amis = (float) -999.0;
    private float k2c = (float) 273.15;

    public USSTLoader(String fname) // usst_raw.2016101800.gdat
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
            usst = new float[(int) nsize >> 2];
            dbuf.get(usst);
            for (int i = 0; i < dbuf.capacity(); i++)
            {
                if (usst[i] > bval)
                {
                    usst[i] = amis;
                }
                else
                {
                    usst[i] -= k2c;
                }
            }
            //usst = data;

            channel.close();
            file.close();
        }
        catch (IOException e)
        {
            throw new DaemonException("USSTLoader error.", e);
        }
    }

    public float[] getData()
    {
        return usst;
    }

    //
    public float GetValue(float lat, float lon)
    {
        int j = (int) ((180.0f - (90.0f - lat)) / 0.05);
        int i = (int) (lon / 0.05);
        return usst[j * mt_width + i];
    }
}