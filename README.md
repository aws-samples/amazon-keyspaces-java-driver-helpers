#Amazon Keyspaces Java Driver Helpers
This repository contains driver policies, examples, and best practices when using the DataStax Java Driver with Amazon Keyspaces (for Apache Cassandra).

## Retry Policies
The DataStax java driver will attempt to retry idempotent request transparently to the application. If you are seeing NoHostAvailableException when using Amazon Keyspaces, replacing the default retry policy with the ones provided in this repository will be beneficial.

Implementing a driver retry policy is not a replacement for an application level retry. Users of Apache Cassandra or Amazon Keyspaces should implement an application level retry mechanism for request that satisfy the applications business requirements.  Additionally, adding complex logic, sleeps or blocking calls in a Driver retry policy should be used with caution.  

### AmazonKeyspacesRetryPolicy
The Amazon Keyspaces Retry Policy is an alternative to the DefaultRetryPolicy for the Cassandra Driver. The main difference from the DefaultRetryPolicy, is the AmazonKeyspacesRetryPolicy will retry request a configurable number of times. By default, we take a conservative approach of 3 retry attempts. This driver retry policy will not throw a NoHostAvailableException. Instead, this retry policy will pass back the original exception sent back from the service.  

The following code shows how to include the  AmazonKeyspacesRetryPolicy to existing configuration

```
#Set idempotence for all operations you application can change on request
basic.request.default-idempotence = true
   advanced.retry-policy {
     class =  com.aws.ssa.keyspaces.retry.AmazonKeyspacesRetryPolicy
     max-attempts = 3
}
```

# Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

# License

This library is licensed under the MIT-0 License. See the LICENSE file.
