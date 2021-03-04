# streamstats

This repository includes a UDX derived from ThroughputTest.geRPS.

As well as counting rows per second, it counts rowtime bounds (and timeouts).

To build:
```
   . /etc/sqlstream/environment
   export SQLSTREAM_HOME
   ./getLocalLibs.sh`
    ant
```

To install the function in the TEST schema, see `install.sql`

To use the function, here is an example based on the buses input stream:

```
select stream * 
from stream(
    test.streamstats
      (cursor (select stream rowtime,"id" from "StreamLab_Output_b"."source_1_ns")
    ));
```

