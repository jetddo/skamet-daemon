package kama.daemon.common.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;

/**
 * @author chlee
 * Created on 2017-01-04.
 * tar.gz 등 압축파일 풀어주는 클래스
 */
public class GZipTgzReader
{
    private String _fileName;
    private File _gZipFile;

    public GZipTgzReader(String fileName) throws FileNotFoundException
    {
        _fileName = fileName;
        _gZipFile = new File(fileName);

        if (!_gZipFile.exists())
        {
            throw new FileNotFoundException(String.format("File %s not found.", _gZipFile.getAbsoluteFile()));
        }
    }

    public byte[] readAllBytes() throws IOException
    {
        try (GZIPInputStream gis = new GZIPInputStream(new FileInputStream(_gZipFile));
             ByteArrayOutputStream bos = new ByteArrayOutputStream())
        {
            ArrayList<Byte> bufferList = new ArrayList<>();
            byte[] buffer = new byte[512];
            int len;

            while ((len = gis.read(buffer)) != -1)
            {
                bos.write(buffer, 0, len);
            }

            buffer = bos.toByteArray();

            gis.close();
            bos.close();
            
            return buffer;
        }
    }

    public void extractGZipToFile(File outputFile) throws IOException
    {
        try (GZIPInputStream gis = new GZIPInputStream(new FileInputStream(_gZipFile));
            FileOutputStream fos = new FileOutputStream(outputFile))
        {
            byte[] buffer = new byte[1024];
            int len;

            while ((len = gis.read(buffer)) != -1)
            {
                fos.write(buffer, 0, len);
            }
            
            gis.close();
            fos.close();
        }
    }

    public File[] extractTgzToDirectory(File baseDirectory) throws IOException
    {
        List<File> extractedFileList = new ArrayList<>();
        TarArchiveInputStream in =
                new TarArchiveInputStream(new GZIPInputStream(new FileInputStream(_gZipFile)));

        TarArchiveEntry entry;

        while (null != (entry = in.getNextTarEntry()))
        {
            if (entry.isDirectory())
            {
                entry = in.getNextTarEntry();
                continue;
            }

            if (in.canReadEntryData(entry))
            {
                File fileToExtract = new File(entry.getName());
                File curFile = new File(baseDirectory, fileToExtract.getName());

                // 파일이 이미 존재할경우 삭제
                if (curFile.exists())
                {
                    curFile.delete();
                }

                // 압축 풀기
                try {
                	
                	FileOutputStream fout = new FileOutputStream(curFile);
               
                    IOUtils.copy(in, fout);
                    extractedFileList.add(curFile);
                    
                    fout.close();
                    
                } catch (Exception e) {
                	
                	Log.print("ERROR : " + e.getMessage());
                	e.printStackTrace();
                }
            } else {
            	Log.print("ERROR : canReadEntryData = false, [" + _gZipFile.getAbsolutePath() + "]");
            }
        }

        File[] files;
        files = new File[extractedFileList.size()];
        
        in.close();

        return extractedFileList.toArray(files);
    }
}
