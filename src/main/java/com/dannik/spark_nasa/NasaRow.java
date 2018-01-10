package com.dannik.spark_nasa;

import java.io.Serializable;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;


abstract class NasaRow implements Serializable, Comparable<NasaRow> {


    static DateTimeFormatter oldFormat = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.UK);
    static DateTimeFormatter newFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    static ZoneId moscow = ZoneId.of("Europe/Moscow");

    protected String url;
    protected int code;
    protected String method;
    protected String date;
    protected int count;



    NasaRow(){
        url = "";
        code = 0;
        method = "";
        date = "";
    }

    protected static String[] parseRow(String row){

        String[] result = new String[4];

        int dateStart = row.indexOf('[') + 1;
        int dateEnd = row.indexOf(']');
        int bracketsStart = row.indexOf('"', dateEnd) + 1;
        int bracketsEnd = row.indexOf('"', bracketsStart);
        int codeStart = row.lastIndexOf('"') + 2;
        int codeEnd = row.indexOf(' ', codeStart);

        String[] brackets = row.substring(bracketsStart, bracketsEnd).split(" ");
        if(brackets.length >= 3){
            result[1] = brackets[0];
            result[2] = brackets[1];
        }else if(brackets.length == 2){
            if(brackets[0].equals(brackets[0].toUpperCase())){
                result[1] = brackets[0];
                result[2] = brackets[1];
            }else{
                result[1] = "NONE";
                result[2] = brackets[0];
            }

        }else if(brackets.length == 1){
            result[1] = "NONE";
            result[2] = brackets[0];
        }

        result[0] = row.substring(dateStart, dateEnd);
        result[3] = row.substring(codeStart, codeEnd);

        return result;
    }




    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getCount() { return count; }
    public void setCount(int count) { this.count = count; }
}