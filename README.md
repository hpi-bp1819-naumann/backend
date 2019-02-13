# backend

> backend for [deequ](https://github.com/hpi-bp1819-naumann/deequ)

## Build Setup

1. Install `sbt`
2. Clone project in the same directory as [deequ](https://github.com/hpi-bp1819-naumann/deequ)
    * Install [deequ](https://github.com/hpi-bp1819-naumann/deequ) if you haven't done so yet
3. Change to `backend` directory
4. Run `importDeequFatJar.sh`

## Start the server

1. Via IntelliJ
* Run `src/main/scala/com/amazon/deequ/backend/JettyLauncher.scala`

2. Via command line
``` bash
sbt
# in the sbt shell
jetty; start
```
The server will then run on `localhost:8080`.

### Frontend Tool

Use the [frontend](https://github.com/hpi-bp1819-naumann/frontend) for easy use.
