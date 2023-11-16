package model;

public class DateInfo {
  private int year;
  private String month;
  private int week;

  public DateInfo(int year) {
    this.year = year;
  }

  public DateInfo(int year, String month) {
    this.year = year;
    this.month = month;
  }

  public DateInfo(int year, String month, int week) {
    this.year = year;
    this.month = month;
    this.week = week;
  }

  @Override
  public String toString() {
    if(week != 0){
      return "Week "+week+" "+month+" "+year;
    }
    else if(month != null){
      return month+" "+year;
    }
    else{
      return String.valueOf(year);
    }
  }
}
