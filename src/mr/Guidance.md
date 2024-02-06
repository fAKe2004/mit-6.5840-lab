1. go to directory : src/
2. run 
  go -race run main/mrcoordinator.go 
3. run (build plugin)
  go build -buildmode=plugin ../mrapps/wc.go
4. run multiple tasks (in other windows)
  go run main/mrworker.go


