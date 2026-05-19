# Go-Cryptomeria

> [!WARNING]
> It's just an POC using Go. The design has several problems and the API is not stable. It's just a bag of ideas.
> Don't use in production. 

In this step, the main goal is to read a parquet file with raw trades and to simulate trades using fixed rules.
I chose GO lang because I believe that a good trading system needs to work using reactive approach and continuous flow (and async channel is the principal citizen when using Go). There are many other libraries and app solutions, but they force an pull-and-block-loop approach which I disagree with when it is necessary to make quick decisions about market flow.