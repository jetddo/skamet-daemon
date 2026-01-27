package kama.daemon.model.observation.adopt.RDR.data;

import org.apache.commons.lang3.ArrayUtils;

/**
 * @author chlee
 * Created on 2016-12-15.
 * 2차원 배열을 처리하는 클래스 (x, y)
 */
public class Layer2D implements Cloneable
{
    protected int[] _raw;
    protected int _sizeX;
    protected int _sizeY;

    private Layer2D()
    {

    }

    protected Layer2D(Layer2D layer2D)
    {
        _sizeX = layer2D._sizeX;
        _sizeY = layer2D._sizeY;
        _raw = layer2D._raw.clone();
    }

    public Layer2D(int width, int height)
    {
        _raw = new int[width * (height + 1)];
        _sizeX = width;
        _sizeY = height;
    }

    public boolean isValidCoordinate(int x, int y)
    {
        boolean isValid = true;

        isValid &= (x > 0) && (x < _sizeX);
        isValid &= (y > 0) && (y < _sizeY);

        return isValid;
    }

    public void setValue(int x, int y, int value)
    {
        _raw[x + y * _sizeX] = value;
    }

    public int getValue(int x, int y)
    {
        return _raw[x + y * _sizeX];
    }

    public byte getByteValue(int x, int y)
    {
        return (byte)getValue(x, y);
    }

    public int[] getVertical(int x)
    {
        int[] buffer = new int[_sizeY];

        for (int y  = 0; y < _sizeY; y++)
        {
            buffer[y] = getValue(x, y);
        }

        return buffer;
    }

    public void flipVertical()
    {
        int[] buffer = new int[_sizeY];

        for (int x  = 0; x < _sizeX; x++)
        {
            buffer = getVertical(x);
            ArrayUtils.reverse(buffer);
            setVerticalValues(x, buffer);
        }
    }

    public void setVerticalValues(int x, int[] buffer)
    {
        int minY = buffer.length > _sizeY ? _sizeY : buffer.length;

        for (int y = 0; y < minY; y++)
        {
            setValue(x, y, buffer[y]);
        }
    }

    public void setHorizontalValues(int y, int[] buffer)
    {
        int minX = buffer.length > _sizeX ? _sizeX : buffer.length;

        for (int x = 0; x < minX; x++)
        {
            setValue(x, y, buffer[y]);
        }
    }

    public void flipHorizontal()
    {
        int[] buffer = new int[_sizeX];

        for (int y  = 0; y < _sizeY; y++)
        {
            buffer = getHorizontal(y);
            ArrayUtils.reverse(buffer);
            setHorizontalValues(y, buffer);
        }
    }

    public int[] getHorizontal(int y)
    {
        int[] buffer = new int[_sizeY];

        for (int x  = 0; y < _sizeY; x++)
        {
            buffer[x] = getValue(x, y);
        }

        return buffer;
    }

    public int getSizeX()
    {
        return _sizeX;
    }

    public int getSizeY()
    {
        return _sizeY;
    }

    public static int stou(byte signed)
    {
        return signed & 0xFF;
    }

    public static byte utos(int unsigned)
    {
        return (byte)(unsigned & 0xFF);
    }

    @Override
    public Layer2D clone()
    {
        Layer2D layer2D;

        layer2D = new Layer2D();

        layer2D._raw = _raw.clone();
        layer2D._sizeX = _sizeX;
        layer2D._sizeY = _sizeY;

        return layer2D;
    }
}

