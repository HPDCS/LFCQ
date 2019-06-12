#ifndef __MATH_HPCDS__
#define __MATH_HPCDS__

extern double uniform_rand(struct drand48_data *seed, double mean);
extern double neg_triangular_rand(struct drand48_data *seed, double mean);
extern double triangular_rand(struct drand48_data *seed, double mean);
extern double exponential_rand(struct drand48_data *seed, double mean);
extern double camel_rand(struct drand48_data *seed, double mean, double overall_cluster_density, int num_clusters, double cluster_length);
extern double camel_compile_time_rand(struct drand48_data *seed, double mean);































#endif
