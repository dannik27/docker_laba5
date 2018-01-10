package com.dannik.spark_nasa;

import java.time.ZonedDateTime;

public class NasaRowSecond extends NasaRow {



    NasaRowSecond(String row){

        String[] words = NasaRow.parseRow(row);
        String parsedDate = newFormat.format(ZonedDateTime.parse(words[0], oldFormat).withZoneSameInstant(moscow));

        setUrl(words[2]);
        setCode(Integer.valueOf(words[3]));
        setMethod(words[1]);
        setDate(parsedDate);

    }


    @Override
    public int hashCode() {
        return getDate().hashCode() + getCode() + getMethod().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return getDate().equals(((NasaRow)obj).getDate()) &&
                getCode() == ((NasaRow)obj).getCode() &&
                getMethod().equals(((NasaRow)obj).getMethod());
    }

    @Override
    public int compareTo(NasaRow obj) {
        return getDate().compareToIgnoreCase(obj.getDate());
    }

    @Override
    public String toString() {
        //return String.format("[%s] %s::%d -->", getDate(), getMethod(), getCode());
        return String.format("%s %s %d", getDate(), getMethod(), getCode());
    }

}
