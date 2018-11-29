Blobcached
=====
Blobcached is a memcached protocol-compatible cache server for blob on SSD.

### Supported commands
| Command | Format |
| ------ | ------ |
| get | get <key> [<key>]+\r\n |
| set | set <key> <flags> <expiry> <datalen> [noreply]\r\n<data>\r\n |
| delete | delete <key> [noreply]\r\n  |
| touch | touch <key> <expiry>[noreply]\r\n  |
| stats | stats\r\n   |

### How it works
#### concepts
| Name |  |
| ------ | ------ |
| indexfile | an indexfile contains many of `items` powered by [blotdb](https://github.com/boltdb/bolt) |
| datafile | a regular file for storing values |
| item | an item is made up of `key`, `offset`, `term`, `size` anchoring the value in datafile |
| term | everytime the `datafile` is full, the `term` of `datafile` is increased  |

#### Command: Set
* get the `offset` and `term` of `datafile`
* write value to the `datafile`
* write `item` with the `offset`, `term` and `key` to the `indexfile`

#### Command: Get 
* get the `item` by `key`
* check `term` and `offset` of the `item` against `datafile` 
* read value from the `datafile`

#### Command: Touch
* implemented by `get` & `set`

#### GC
* Blobcached scans and removes expired or invalid `items` in the `indexfile`
* by default, the rate up to 32k items/second
