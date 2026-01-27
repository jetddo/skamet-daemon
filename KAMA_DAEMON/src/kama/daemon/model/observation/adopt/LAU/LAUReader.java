package kama.daemon.model.observation.adopt.LAU;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author chlee
 * Created on 2016-12-05.
 */
public class LAUReader
{
    private static final int HEAD2_BYTE = 11;
    private static final int END2_BYTE = 4;
    private static final int AWS2_BYTE = 103; // Total bytes per AWS
    private static final int AWS2_OPP = 21; // AWS essential data count
    private static final int AWS2_OPT = 23; // AWS optional data count

    public LAUReader()
    {

    }

    public List<LAUData> retrieveData(String[] files) throws IOException
    {
        List<LAUData> lstLAUData;

        lstLAUData = new ArrayList<LAUData>();

        for (String file : files)
        {
            lstLAUData.addAll(parseFile(file));
        }

        return lstLAUData;
    }

    public List<LAUData> retrieveData(String file) throws IOException
    {
        return parseFile(file);
    }

    private static String[] getFiles(final String path, final String searchPattern)
    {
        final Pattern ptn;
        String[] sFiles;

        ptn = Pattern.compile(searchPattern.replace("*", ".*").replace("?", ".?"));

        sFiles = new File(path).list(new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                return new File(dir, name).isFile() && ptn.matcher(name).matches();
            }
        });

        return sFiles;
    }

    public List<LAUData> parseFile(String fileName) throws IOException
    {
        int[] buffer;
        Path path;
        List<LAUData> lstLAUData;
        LAUHeader lauHeader;
        LAUData lauData;

        lauData = new LAUData();
        lstLAUData = new ArrayList<LAUData>();

        buffer = byteToInt(Files.readAllBytes(Paths.get(fileName)));
        lauHeader = readHeader(buffer);

        buffer = truncateBuffer(HEAD2_BYTE, buffer);

        for (int k = 0; k < lauHeader.NumOfAWSs; k++)
        {
            lauData = readData(buffer);
            lauData.Header = lauHeader;

            lstLAUData.add(lauData);
            buffer = truncateBuffer(AWS2_BYTE, buffer);
        }

        return lstLAUData;
    }

    private LAUHeader readHeader(int[] buffer)
    {
        LAUHeader lauHeader;

        lauHeader = new LAUHeader();

        lauHeader.LAUID = (buffer[2] & 0x3F) * 256 + buffer[3];
        lauHeader.Year = (buffer[4] & 0x7F);
        lauHeader.Year += 2000;
        lauHeader.Month = buffer[5] & 0x0F;
        lauHeader.Day = buffer[6] & 0x1F;
        //lauHeader.Minute = buffer[7] & 0x1F;
        lauHeader.NumOfAWSs = (buffer.length - HEAD2_BYTE - END2_BYTE) / AWS2_BYTE;

        return lauHeader;
    }

    private LAUData readData(int[] buffer)
    {
        LAUData lauData;
        int k, i;

        lauData = new LAUData();

        lauData.ProtocolYear = buffer[0] & 0x7F;
        lauData.ProtocolYear += 2000;
        lauData.ProtocolMonth = buffer[1] & 0x0F;
        lauData.ProtocolDay = buffer[2] & 0x1F;
        lauData.AWSYear = buffer[3] & 0x7F;
        lauData.AWSYear += 2000;
        lauData.AWSMonth = buffer[4] & 0x0F;
        lauData.AWSDay = buffer[5] & 0x1F;
        lauData.AWSHour = buffer[6] & 0x1F;
        lauData.AWSMinute = buffer[7] & 0x3F;

        lauData.DataType = buffer[8] & 0xFF; // 자료 구분 (I, B, Q)
        lauData.DataTypeIndex = buffer[9] & 0x03; // 자료 형식 번호 (0: 필수/선택, 1: 필수, 2:강수량)
        lauData.AWSID = (buffer[10] & 0x7F) * 256 + buffer[11];

        k = 11;
        i = 12;
        lauData.FooterData = new ArrayList<Integer>();

        // Data area begins
        if (lauData.DataTypeIndex == 0)
        {
            int lastIndex;

            for (int j = k; j < (k + AWS2_OPP + AWS2_OPT); j++)
            {
                lastIndex = lauData.FooterData.size();

                lauData.FooterData.add(buffer[i] * 256 + buffer[i + 1]);
                i += 2;
            }

            lauData.FooterV1 = buffer[i] & 0xFF;
            i++;
            lauData.FooterV2 = (buffer[i] & 0xFF) * 256 + (buffer[i + 1] & 0xFF);
        }
        else if (lauData.DataTypeIndex == 1)
        {
            for (int j = k; j < (k + AWS2_OPP); j++)
            {
                lauData.FooterData.add(buffer[i] * 256 + buffer[i + 1]);
                i += 2;
            }

            lauData.FooterV1 = buffer[i] & 0xFF;
            i++;
            lauData.FooterV2 = (buffer[i] & 0xFF) * 256 + (buffer[i + 1] & 0xFF);
        }

        return lauData;
    }

    private int[] truncateBuffer(int startIndex, int[] buffer)
    {
        int[] cpyBuffer;
        cpyBuffer = new int[buffer.length - startIndex];

        cpyBuffer = Arrays.copyOfRange(buffer, startIndex, buffer.length - 1);

        return cpyBuffer;
    }

    private int[] byteToInt(byte[] buffer)
    {
        int[] output;

        output = new int[buffer.length];

        for (int i = 0; i < buffer.length; i++)
        {
            if (buffer[i] < 0)
            {
                output[i] = buffer[i] + 256;
            }
            else
            {
                output[i] = buffer[i];
            }
        }

        return output;
    }

    public int aws2_rcv_dec()
    {
        return 0;
    }
}