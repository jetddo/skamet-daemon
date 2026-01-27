package kama.daemon.common.util.converter;

/*
 * Copyright (C) 2007 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.awt.*;
import java.awt.geom.Point2D;

/**
 * PointF holds two float coordinates
 * 소스 출처: https://github.com/treyzania/SpecialSnake/blob/master/src/com/treyzania/specialsnake/core/PointF.java
 */
public class PointF extends Point2D implements java.io.Serializable
{
    public double x;
    public double y;

    public PointF()
    {
    }

    @Override
    public double getX()
    {
        return x;
    }

    @Override
    public double getY()
    {
        return y;
    }

    @Override
    public void setLocation(double x, double y)
    {

    }

    public PointF(double x, double y)
    {
        this.x = x;
        this.y = y;
    }

    public PointF(Point p)
    {
        this.x = p.x;
        this.y = p.y;
    }

    /**
     * Set the point's x and y coordinates
     */
    public final void set(double x, double y)
    {
        this.x = x;
        this.y = y;
    }

    /**
     * Set the point's x and y coordinates to the coordinates of p
     */
    public final void set(PointF p)
    {
        this.x = p.x;
        this.y = p.y;
    }

    public final void negate()
    {
        x = -x;
        y = -y;
    }

    public final void offset(double dx, double dy)
    {
        x += dx;
        y += dy;
    }

    /**
     * Returns true if the point's coordinates equal (x,y)
     */
    public final boolean equals(double x, double y)
    {
        return (this.x == x && this.y == y);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        PointF pointF = (PointF) o;

        if ((pointF.x - x) != 0)
        {
            return false;
        }

        if ((pointF.y - y)  != 0)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        long result = (x != +0.0f ? java.lang.Double.doubleToLongBits(x) : 0);
        result = 31 * result + (y != +0.0f ? java.lang.Double.doubleToLongBits(y) : 0);

        if (result > Integer.MAX_VALUE)
        {
            result = Integer.MAX_VALUE;
        }
        else if (result < Integer.MIN_VALUE)
        {
            result = Integer.MIN_VALUE;
        }

        return (int)result;
    }

    @Override
    public String toString()
    {
        return "PointF(" + x + ", " + y + ")";
    }

    /**
     * Return the euclidian distance from (0,0) to the point
     */
    public final double length()
    {
        return length(x, y);
    }

    /**
     * Returns the euclidian distance from (0,0) to (x,y)
     */
    public static double length(double x, double y)
    {
        return Math.hypot(x, y);
    }
}