
#include <math.h>
#include <stdlib.h>

double uniform_rand(struct drand48_data *seed, double mean)
{
	double random_num = 0.0;
	drand48_r(seed, &random_num);
	random_num *= mean*2;
	return random_num;
}

double neg_triangular_rand(struct drand48_data *seed, double mean)
{
	double random_num = 0.0;
	drand48_r(seed, &random_num);
	random_num = 1-sqrt(random_num);
	random_num *= mean*3;
	return random_num;
}

double triangular_rand(struct drand48_data *seed, double mean)
{
	double random_num = 0.0;
	drand48_r(seed, &random_num);
	random_num = sqrt(random_num);
	random_num *= mean*3/2;
	return random_num;
}

double exponential_rand(struct drand48_data *seed, double mean)
{
	double random_num = 0.0;
	drand48_r(seed, &random_num);
	random_num =  -log(random_num);
	random_num *= mean;
	return random_num;
}

double camel_rand(struct drand48_data *seed, double mean, double overall_cluster_density, int num_clusters, double cluster_length)
{
	double random_num = 0.0;
	double high_density = overall_cluster_density/num_clusters;
	double low_density 	= (1-high_density)/(num_clusters+1);
	double high_length 	= cluster_length;
	double low_length 	= (1-high_length*num_clusters)/(num_clusters+1);
	int i;
	mean*=2;
	drand48_r(seed, &random_num);
	
	for(i=0; i< num_clusters;i++)
	{
		if(random_num < low_density)
			return  mean * random_num * low_length/low_density   +i*(high_length+low_length);
	
		random_num -= low_density;
		
		if(random_num < high_density)
			return  mean * random_num * high_length/high_density +i*(high_length+low_length)+low_length;
	
		random_num -= high_density;
	}
	
	return  mean * random_num * low_length/low_density   +i*(high_length+low_length);
}


double camel_compile_time_rand(struct drand48_data *seed, double mean) 
{
	double overall_cluster_density = 0.999;
	int num_clusters = 2;
	double cluster_length = 0.0005;
	double random_num = 0.0;
	double high_density = overall_cluster_density/num_clusters;
	double low_density 	= (1-high_density)/(num_clusters+1);
	double high_length 	= cluster_length;
	double low_length 	= (1-high_length*num_clusters)/(num_clusters+1);
	int i;
	mean*=2;
	drand48_r(seed, &random_num);
	
	for(i=0; i< num_clusters;i++)
	{
		if(random_num < low_density)
			return  mean * random_num * low_length/low_density   +i*(high_length+low_length);
	
		random_num -= low_density;
		
		if(random_num < high_density)
			return  mean * random_num * high_length/high_density +i*(high_length+low_length)+low_length;
	
		random_num -= high_density;
	}
	
	return  mean * random_num * low_length/low_density   +i*(high_length+low_length);
}



double zipf_rand(struct drand48_data *seed, double a){
  double b;

  a = a - 1.0;
  b = pow(2.0, a);
  int count = 1000;
  while (1) {
    double T, U, V, X, VX;
    drand48_r(seed, &U);
    U = 1.0 - U;
    X = pow(U, -1.0 / a);
    if (X < 1.0){
      count-=1;
      if(count<0) printf("failed after 1000 attempts");
      continue;
    }
    drand48_r(seed, &V);
    T = pow(1.0 + 1.0 / X, a);
    VX = V * X;

    if (VX * (T - 1.0) / (b - 1.0) <= T / b) {
      printf("%f\n",X);
      return X;
    }
  }
}


double zipf_compile_time_rand(struct drand48_data *seed, double a){
 return zipf_rand(seed, 5);
}



double pareto_compile_time_rand(struct drand48_data *seed, double mean){
  double R;
  drand48_r(seed, &R);
  return mean * 0.75 * pow(R, -0.25);
}




