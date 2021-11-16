# I/O Interfaces

MuMMI offers a consistent I/O API with easily switchable backends. Currently, 
there are three backends.

|           | Save Location           | Advantage |
|-----------|-------------------------|---|
| `IO_Simple` | on the file system      | Easy to edit and view data |
| `IO_Tar`    | in a compressed tarball | High scalability with low overhead |
| `IO_Redis`  | in a Redis database     | Extreme scalability |

MuMMI uses the notion of a "namespace" to store the data. For `IO_Simple`, this 
is simply a path to a directory. For `IO_Tar`, the namespace is the path to a 
tar file. For `IO_Redis`, this can be any string identifier. Within each namespace, 
the interface treats data as key-value pairs. In the context of files, a key 
corresponds to a filename and value to the content (ascii or binary) of the file.

### Launching Redis Nodes
`IO_Redis` requires a `redis` server, which can be launched using the following 
script. 
```
MUMMI_CORE = /path/to/repo
MUMMI_REDIS_NODES = n       # number of nodes for the redis server
source $MUMMI_CORE/setup/redis/start_all_redis_nodes.sh $MUMMI_REDIS_NNODES
```

### Usage

```
import mummi_core

namespace = '/home/test_dir'
io_interface = mummi_core.get_io('simple')

io_interface.save_files(namespace, ['key1', 'key2'], ['value1', 'value2'])
data = io_interface.load_files(namespace, ['key1', 'key2'])

# Same functionality can be used for other backends by 
# simply initializing a different interface
namespace = '/home/test_dir/test_file.tar'
io_interface = mummi_core.get_io('taridx')

# or
namespace = 'test_redis_namespace'
io_interface = mummi_core.get_io('redis')
```

#### API 

##### `get_type() => str`
<!-- ##### :warning: Can freeze your browser if you open the Developer Tools. -->

Returns `simple`, `taridx`, or `redis`.

##### `check_environment() => bool`
Checks if the environment is configured correctly (useful only for `IO_Redis').

##### `file_exists(namespace: str, key: str) => bool`
Checks if key exists in file system directory or database.

##### `namespace_exists(namespace: str) => bool`
Checks if namespace exists in file system directory or database.

##### `list_keys(namespace: str, keypattern: str) => list`
Returns list of all keys at namespace.

##### `move_key(namespace: str, key: str, prefix="done", suffix=".npz")`
Renames key with `prefix` and `suffix` (only for `IO_Simple`).

##### `save_files(namespace: str, keys: str/list, data)`
If `keys` is a `str`, saves data to that key.
If `keys` is a `list`, saves `list` data to respective keys (e.g., data[0] is stored at keys[0]).

##### `load_files(namespace: str, keys: str/list) => data`
Loads items from respective key/keys.

##### `save_npz(namespace: str, key: str/list, data, writer_func=write_npz)`
Saves to a `.npz` archive using `writer_func`.

##### `load_npz(namespace: str, key: str/list, reader_func=read_npz) => dict`
Loads from a `.npz` archive using `reader_func`.

##### `remove_files(namespace: str, keys: str/list)`
Removes a file or a list of files from the given namespace.

##### `take_backup(filename: str)`
Backs up file at `filename` + `.bak`.

##### `save_checkpoint(filename: str, data: dict)`
Saves checkpoint to a file.

##### `load_checkpoint(filename: str) => dict`
Loads checkpoint from a file.

##### `send_signal(path: str, key: str)`
Create a signal file on the filesystem (file with a single character).

##### `test_signal(path: str, key: str) => bool`
Checks signal by searching for file at path.