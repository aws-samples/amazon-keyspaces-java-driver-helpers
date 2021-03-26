# Amazon Keyspaces Java Driver Helpers
This repository contains driver policies, examples, and best practices when using the DataStax Java Driver with Amazon Keyspaces (for Apache Cassandra).

## Retry Policies
The DataStax java driver will attempt to retry idempotent request transparently to the application. If you are seeing NoHostAvailableException when using Amazon Keyspaces, replacing the default retry policy with the ones provided in this repository will be beneficial.

Implementing a driver retry policy is not a replacement for an application level retry. Users of Apache Cassandra or Amazon Keyspaces should implement an application level retry mechanism for request that satisfy the applications business requirements.  Additionally, adding complex logic, sleeps or blocking calls in a Driver retry policy should be used with caution.  

### AmazonKeyspacesRetryPolicy
The Amazon Keyspaces Retry Policy is an alternative to the DefaultRetryPolicy for the Cassandra Driver. The main difference from the DefaultRetryPolicy, is the AmazonKeyspacesRetryPolicy will retry request a configurable number of times. By default, we take a conservative approach of 3 retry attempts. This driver retry policy will not throw a NoHostAvailableException. Instead, this retry policy will pass back the original exception sent back from the service.  

The following code shows how to include the  AmazonKeyspacesRetryPolicy to existing configuration

```
   advanced.retry-policy {
     class =  com.aws.ssa.keyspaces.retry.AmazonKeyspacesRetryPolicy
     max-attempts = 3
}
```

## Throttling / RateLimiting
Retries maintain level of availability when receiving short burst of traffic, acute failure, or loss of connection, but sustained retries can further destabilize systems resulting in cascading failure. If you are using retries to limit traffic then you may want to consider a rate limiter.  As reties continue to occur at a steady rate they increasingly add to the overall traffic sent to the database.  When facing this scenario you should introduce rate-limiting. Rate limiters provide what is known as back pressure. You can achieve this by leveraging the Java Driver's Throttler Extension point.  There are a few rate-limiters provided native with the driver, but in this repository we will provide some sample limiters that are designed for Amazon Keyspaces serverless capacity and service quotas. 

### AmazonKeyspacesFixedRateThrottler
This is a request throttler that limits the rate of requests per second. It can be used to match the current table provisioned rate if you have predictable or average capacity per request.
The limit is configurable through the client driver configuration. 
The rate of request is controlled by Guava SmoothBursty Ratelimiter that allows two minutes of capacity to aggregate if not used. 
The second limiter is dynamically configured based on the number of connections defined in the pool setting that limits overall throughput during burst behavior. 
The limiter will control the number of cql request per second but expects the table to have proper capacity defined to achieve utilization. 
This is a blocking implementation, but has a configurable timeout. 
    
The most well-known use-case for this type of rate limiter is bulk loading data at consistent rates or batch processing. In the image below we are able to utilize 100 percent of the table's capacity without error by fixing the request rate to the table write capacity. The workload gradually stepped up throughput level all the way up to 3x the request rate of the table's provisioned capacity. Additionally, since the rate limiter allows for bursting, we are able to use burst capacity of the table that can accrue when provisioned capacity is not fully utilized.  Eventually, the blue line for provision capacity and the green line for provisioned capacity are one to one. The fixed are limiter can also be used with On-Demand Capacity Tables to fit within current account specific table quotas.  

![Rate Limiting](/static/images/RateLimiting.png) 
 
To activate this throttler, modify the {@code advanced.throttler} section in the driver configuration, for example:
     
   ```
      datastax-java-driver {
         advanced.throttler = {
                class = com.aws.ssa.keyspaces.throttler.AmazonKeyspacesFixedRateThrottler
                max-requests-per-second = 1000
                endpoint-type = VPC
                register-timeout = 1 seconds
          }
      }  
```



* `max-requests-per-second` : controls the request rate. Blocks until available permits or timeout it reached
* `endpoint-type` : if you are connected to the VPC endpoint or the public endpoint. Used to determine throughput based on the number of connections specified in:`advanced.connection.pool.local.size`
* `register-timeout` timeout waiting for permits. Should be less than or equal to `basic.request.timeout'


# Build this project
To build and use this library execute the following mvn command and place on the classpath of your application. 
```
mvn clean package
```
# Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

# License

This library is licensed under the MIT-0 License. See the LICENSE file.
