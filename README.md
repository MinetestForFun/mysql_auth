# MySQL Authentication
[![Build Status](https://travis-ci.org/MinetestForFun/mysql_auth.svg)](https://travis-ci.org/MinetestForFun/mysql_auth)

Plug Minetest's auth mechanism into a MySQL database.

## Configuration

First, if mod security is enabled (`secure.enable_security = true`), this mod must be added as
a trusted mod (in the `secure.trusted_mods` config entry). There is **no** other solution to
make it work under mod security.

By default, `mysql_auth` doesn't run in singleplayer. This can be overriden by setting
`mysql_auth.enable_singleplayer` to `true`.

Configuration may be done as regular Minetest settings entries, or using a config file, allowing
for more configuration options; to do so specify the path as `mysql_auth.cfgfile`. This config
must contain a Lua table that can be read by `minetest.deserialize`, i.e. a regular table
definition follwing a `return` statement (see the example below).

When using flat Minetest configuation entries, all the following option names must be prefixed
with `mysql_auth.`. When using a config file, entries are to be hierarchised as per the dot
separator.

Values written next to option names are default values.

## Database connection

### Flat config file

```lua
db.host = 'localhost'
db.user = nil -- MySQL connector defaults to current username
db.pass = nil -- Using password: NO
db.port = nil -- MySQL connector defaults to either 3306, or no port if using localhost/unix socket
db.db = nil -- <== Setting this is required
```

### Lua table config file

Connection options are passed as a table through the `db.connopts` entry.
Its format must be the same as [LuaPower's MySQL module `mysql.connect(options_t)` function][mycn],
that is (all members are optional);

```lua
connopts = {
  host = ...,
  user = ...,
  pass = ...,
  db = ...,
  port = ...,
  unix_socket = ...,
  flags = { ... },
  options = { ... },
  attrs = { ... },
  -- Also key, cert, ca, cpath, cipher
}
```

## Auth table schema finetuning

```lua
db.tables.auths.name = 'auths'
db.tables.auths.schema.userid = 'userid'
db.tables.auths.schema.userid_type = 'INT'
db.tables.auths.schema.username = 'username'
db.tables.auths.schema.username_type = 'VARCHAR(32)'
db.tables.auths.schema.password = 'password'
db.tables.auths.schema.password_type = 'VARCHAR(512)'
db.tables.auths.schema.privs = 'privs'
db.tables.auths.schema.privs_type = 'VARCHAR(512)'
db.tables.auths.schema.lastlogin = 'lastlogin'
db.tables.auths.schema.lastlogin_type = 'BIGINT'
```

The `_type` config entries are only used when creating an auth table, i.e. when
`db.tables.auths.name` doesn't exist.

## Examples

### Example 1

#### Using a Lua config file

`minetest.conf`:
```
mysql_auth.cfgfile = /srv/minetest/skyblock/mysql_auth_config
```

`/srv/minetest/skyblock/mysql_auth_config`:
```lua
return {
  db = {
    connopts = {
      user = 'minetest',
      pass = 'BQy77wK$Um6es3Bi($iZ*w3N',
      db = 'minetest'
    },
    tables = {
      auths = {
        name = 'skyblock_auths'
      }
    }
  }
}
```

#### Using only Minetest config entries

`minetest.conf`:
```
mysql_auth.db.user = minetest
mysql_auth.db.pass = BQy77wK$Um6es3Bi($iZ*w3N
mysql_auth.db.db = minetest
mysql_auth.db.tables.auth.name = skyblock_auths
```

## License

`mysql_auth` is licensed under [GNU LGPLv3](https://www.gnu.org/licenses/lgpl.html).

Using the Public Domain-licensed LuaPower `mysql` module.


[mycn]: https://luapower.com/mysql#mysql.connectoptions_t---conn