To run use `sbt "run {filename}"`.

It is implemented assuming input file contains sorted by time data.

In case input data is not valid, this line is completely ignored.
Time window moving can be shifted if there is a time gap in data, longer than 2 * window size.
Not all corner cases are covered - like what exactly to do with 0 values or invalid values and 
if the window should be moved strictly regardless if there is data for time in it.
