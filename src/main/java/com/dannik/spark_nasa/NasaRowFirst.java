package com.dannik.spark_nasa;

public class NasaRowFirst extends NasaRow {


    NasaRowFirst(String row){
        super();

        String[] words = NasaRow.parseRow(row);
        setUrl(words[2]);
        setCode(Integer.valueOf(words[3]));
        setMethod(words[1]);

    }

    @Override
    public int hashCode() {
        return getUrl().hashCode() + getCode() + getMethod().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return getUrl().equals(((NasaRow)obj).getUrl()) &&
                getMethod().equals(((NasaRow)obj).getMethod()) &&
                getCode() == ((NasaRow)obj).getCode();
    }

    @Override
    public int compareTo(NasaRow obj) {
        return getUrl().compareToIgnoreCase(obj.getUrl());
    }

    @Override
    public String toString() {
        return String.format("%s(%s) :%d -->", getUrl(), getMethod(), getCode());
    }
}
