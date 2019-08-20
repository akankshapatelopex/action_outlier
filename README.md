# action_outlier

This action detects outliers in a column of a table and flag it as true in a particular column, indicated as flag column. 

In settings, one needs to specify the column whose values have to be checked for outlier, the method used (details of which are discussed below), the value to be used with the methods, and the column which marks with 'true' if the row has any outlier in the columns which have been tested.

Currently, there are three methods to detect outliers:

i. Range: Given lower bound and upper bound on values of a column, the action flags the values outside the range.

ii. zScore: indicates how many standard deviations the value is away from the mean. So, if we set zScore value to 2.5, any value in the column which is more than [mean + (2.5 * std dev)] or less than [mean - (2.5 * std dev)] is flagged as outlier.

iii. IQR: Inter-quartile range is the differnce between the first quartile (Q1) and the third quartile (Q3). So, if we set IQR to 1.5, any value below [Q1 - (1.5 * IQR)] or greater than [Q3 + (1.5 * IQR)] is flagged as outlier.
