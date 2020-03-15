# Pure golang daemons implementation

### ./crond - cron-daemon

**Example**

```go
package main

import (
	"fmt"
	"github.com/gtmartem/go-daemons/crond"
)

func main() {
	c := crond.NewCron("", f)
	c.Start()
}

func f() {
	fmt.Println("im done")
}
```

**Output**

```
INFO[2020-03-15 21:41:56] starting cron-daemon work                    
im done
^CINFO[2020-03-15 21:42:03] cron-daemon shutdown  
```
