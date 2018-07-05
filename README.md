## Overview

These files clean a small financial dataset using MapReduce. In particular, the goal is to reconstruct missing data from non-trading days (weekends, bank holidays, natural disasters, etc.) by copying values from the previous trading day. The data was scraped from a financial trading site, such as [Yahoo](https://finance.yahoo.com/quote/csv/history/), and can be accessed from [The Ohio State University](https://www.osu.edu/). The zip file should contain files "intc-dividends.csv", "intc-price.csv", "options-price.csv", "spy- dividends.csv", "spy-price.csv". Please put the files in HDFS and place each file in a separate directory.

Dataset: [Scraped Trading Data](http://cse.osu.edu/~sblanas/3244/findata.zip) [253 MB]

## Variable Descriptions

- `Date:` The date of record.
- `Open:` The opening sales price.
- `High:` The highest sales price that the stock has achieved during the regular trading hours.
- `Low:` The lowest sales price that the stock has achieved during the regular trading hours.
- `Close:` The closing sales price that the stock has achieved at the end of the trading session.
- `Adj_Close:` The adjusted closing price that the stock has achieved at the end of the trading session, which includes any distributions and corporate actions that occurred at any time before the next trading session.
- `Volume:` The closing daily volume that the stock has achieved at the end of the trading session.

## File Description for DataClean1.java

DataClean1.java is a MapReduce program that cleans the "spy-price.csv" and "intc-price.csv" files between 2015-01-02 and 2016-06-30. These two files have no price for some dates. For example, the last two lines in file "spy-price.csv" are:
```
2015-01-05,204.169998,204.369995,201.350006,201.720001,169632600,193.48662
2015-01-02,206.380005,206.880005,204.179993,205.429993,121465900,197.045185
```

The reconstructed output should be:
```
2015-01-05,204.169998,204.369995,201.350006,201.720001,169632600,193.48662
2015-01-04,206.380005,206.880005,204.179993,205.429993,121465900,197.045185
2015-01-03,206.380005,206.880005,204.179993,205.429993,121465900,197.045185
2015-01-02,206.380005,206.880005,204.179993,205.429993,121465900,197.045185
```

That is, the trading data for 2015-01-03 and 2015-01-04 come from the data for the 2015-01-02 trading day.
Note that the order the lines appear in the final output does not matter. We only care for the existence of the rows, not their relative order. In summary, this Java file is a MapReduce program that cleans the "spy-price.csv" and "intc- price.csv" files. The same MapReduce program is able to clean both files, in two separate invocations of MapReduce with different input and output directories.
   
## File Description for DataClean2.java

DataClean2.java is a MapReduce program that cleans the "options-price.csv" file between 2015-01-02 and 2016-06-30.
The "options-price.csv" file is also missing pricing information for some days. However, the "options-price.csv" file is unique in that it stores information a little differently. In particular, it stores symbols for different securities in the same file. Consider the following subset of lines in the "options-price.csv" file:
```
2015-01-02,INTC150123C00035000,INTC,2015-01-23,C,35.0,1.77,1.81,1.97
2015-01-02,SPY170120C00260000,SPY,2017-01-20,C,260.0,2.47,1.91,2.71
2015-01-05,SPY170120C00260000,SPY,2017-01-20,C,260.0,1.75,1.45,2.32
2015-01-05,INTC150123C00035000,INTC,2015-01-23,C,35.0,1.77,1.55,1.63
```

The output that reconstructs the missing values would be:
```
2015-01-02,INTC150123C00035000,INTC,2015-01-23,C,35.0,1.77,1.81,1.97
2015-01-02,SPY170120C00260000,SPY,2017-01-20,C,260.0,2.47,1.91,2.71
2015-01-03,INTC150123C00035000,INTC,2015-01-23,C,35.0,1.77,1.81,1.97
2015-01-03,SPY170120C00260000,SPY,2017-01-20,C,260.0,2.47,1.91,2.71
2015-01-04,INTC150123C00035000,INTC,2015-01-23,C,35.0,1.77,1.81,1.97
2015-01-04,SPY170120C00260000,SPY,2017-01-20,C,260.0,2.47,1.91,2.71
2015-01-05,SPY170120C00260000,SPY,2017-01-20,C,260.0,1.75,1.45,2.32
2015-01-05,INTC150123C00035000,INTC,2015-01-23,C,35.0,1.77,1.55,1.63
```

That is, the trading data for 2015-01-03 and 2015-01-04 for the "INTC150123C00035000" security come from the 2015-01-02 trading data for security "INTC150123C00035000". The trading data for 2015-01-03 and 2015-01-04 for the "SPY170120C00260000" security come from the 2015-01-02 trading data for security "SPY170120C00260000" (Note, again, that the order of the lines in the output does not matter). In summary, this Java file is a single MapReduce program that cleans the "options-price.csv" file.
