/*
 * Copyright 2007 University of Waikato.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License.  
 */

package com.github.javacliparser;


/**
 * Float option.
 *
 * @author Richard Kirkby (rkirkby@cs.waikato.ac.nz)
 * @version $Revision: 7 $
 */
public class FloatOption extends AbstractOption {

    private static final long serialVersionUID = 1L;

    protected float currentVal;

    protected float defaultVal;

    protected float minVal;

    protected float maxVal;

    public FloatOption(String name, char cliChar, String purpose,
            float defaultVal) {
        this(name, cliChar, purpose, defaultVal, Float.NEGATIVE_INFINITY,
                Float.POSITIVE_INFINITY);
    }

    public FloatOption(String name, char cliChar, String purpose,
            float defaultVal, float minVal, float maxVal) {
        super(name, cliChar, purpose);
        this.defaultVal = defaultVal;
        this.minVal = minVal;
        this.maxVal = maxVal;
        resetToDefault();
    }

    public void setValue(float v) {
        if (v < this.minVal) {
            throw new IllegalArgumentException("Option " + getName()
                    + " cannot be less than " + this.minVal
                    + ", out of range: " + v);
        }
        if (v > this.maxVal) {
            throw new IllegalArgumentException("Option " + getName()
                    + " cannot be greater than " + this.maxVal
                    + ", out of range: " + v);
        }
        this.currentVal = v;
    }

    public float getValue() {
        return this.currentVal;
    }

    public float getMinValue() {
        return this.minVal;
    }

    public float getMaxValue() {
        return this.maxVal;
    }

    @Override
    public String getDefaultCLIString() {
        return floatToCLIString(this.defaultVal);
    }

    @Override
    public String getValueAsCLIString() {
        return floatToCLIString(this.currentVal);
    }

    @Override
    public void setValueViaCLIString(String s) {
        setValue(cliStringToFloat(s));
    }

    public static float cliStringToFloat(String s) {
        return Float.parseFloat(s.trim());
    }

    public static String floatToCLIString(float d) {
        return Float.toString(d);
    }

    //@Override
    //public JComponent getEditComponent() {
    //    return new FloatOptionEditComponent(this);
    //}
}
