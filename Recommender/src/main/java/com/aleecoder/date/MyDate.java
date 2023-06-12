package com.aleecoder.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;

/**
 * @author HuanyuLee
 * @date 2022/5/26
 */
public class MyDate {

    public static Date getDate(Date date, Integer amount) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, amount);
        return calendar.getTime();
    }

    public static void main(String[] args) throws ParseException {
        Scanner sc = new Scanner(System.in);
        System.out.print("输入配牛时间：");
        String date = sc.nextLine();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date d1 = df.parse(date);
        Date d2 = getDate(d1, 285);
        System.out.println("预计下牛时间：" + df.format(d2));
    }
}
