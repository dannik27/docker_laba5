package com.dannik.spark_nasa;


import java.time.ZonedDateTime;

public class NasaRowThird extends NasaRow{


    NasaRowThird(String line){

        String[] words = NasaRow.parseRow(line);
        String parsedDate = newFormat.format(ZonedDateTime.parse(words[0], oldFormat).withZoneSameInstant(moscow));

        setCode(Integer.valueOf(words[3]));
        setDate(parsedDate);
    }


    @Override
    public int hashCode() {
        return getDate().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return getDate().equals(((NasaRow)obj).getDate());
    }

    @Override
    public int compareTo(NasaRow obj) {
        return getDate().compareToIgnoreCase(obj.getDate());
    }

    @Override
    public String toString() {
        //не нужен
        return String.format("%s %s %d", getDate(), getMethod(), getCode());
    }
}
