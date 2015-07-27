# GenericUDAFCollectSample
A Hive UDAF to collect sample given a sampling probability

provides the following interface:

collect_sample(column_name, sampling_prob)

The sampling probability should be a double type between 0 and 1 inclusive.
