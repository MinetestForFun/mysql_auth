local modname = minetest.get_current_modname()
local modpath = minetest.get_modpath(modname) 

local thismod = {}
_G[modname] = thismod

local singleplayer = minetest.is_singleplayer() -- Caching is OK since you can't open a game to
-- multiplayer unless you restart it.
if not minetest.setting_get(modname .. '.enable_singleplayer') and singleplayer then
  core.log('action', modname .. ": Not adding auth handler because of singleplayer game")
  return
end

local function setoverlay(tab, orig)
  local mt = getmetatable(tab) or {}
  mt.__index = function (tab, key)
    if rawget(tab, key) ~= nil then
      return rawget(tab, key)
    else
      return orig[key]
    end
  end
  setmetatable(tab, mt)
end

local function string_splitdots(s)
  local temp = {}
  local index = 0
  local last_index = string.len(s)
  while true do
    local i, e = string.find(s, '%.', index)
    if i and e then
      local next_index = e + 1
      local word_bound = i - 1
      table.insert(temp, string.sub(s, index, word_bound))
      index = next_index
    else            
      if index > 0 and index <= last_index then
        table.insert(temp, string.sub(s, index, last_index))
      elseif index == 0 then
        temp = nil
      end
      break
    end
  end
  return temp
end

local mysql
do -- MySQL module loading
  local env = {
    require = function (module)
      if module == 'mysql_h' then
        return dofile(modpath .. '/mysql/mysql_h.lua')
      else
        return require(module)
      end
    end
  }
  setoverlay(env, _G)
  local fn, msg = loadfile(modpath .. '/mysql/mysql.lua')
  if not fn then error(msg) end
  setfenv(fn, env)
  local status
  status, mysql = pcall(fn, {})
  if not status then
    error(modname .. ' failed to load MySQL FFI interface: ' .. mysql)
  end
end

do
  local get
  do
    get = function (name) return minetest.setting_get(modname .. '.' .. name) end
    local cfgfile = get('cfgfile')
    if type(cfgfile) == 'string' and cfgfile ~= '' then
      local file = io.open(cfgfile, 'rb')
      if not file then
        error(modname .. ' failed to load specified config file at ' .. cfgfile)
      end
      local cfg, msg = minetest.deserialize(file:read('*a'))
      file:close()
      if not cfg then
        error(modname .. ' failed to parse specified config file at ' .. cfgfile .. ': ' .. msg)
      end
      get = function (name)
        if type(name) ~= 'string' or name == '' then
          return nil
        end
        local parts = string_splitdots(name)
        local tbl = cfg[parts[1]]
        for n = 2, #parts do
          if tbl == nil then
            return nil
          end
          tbl = tbl[parts[n]]
        end
        return tbl
      end
    end
  end

  local conn
  do
    -- MySQL API backend
    mysql.config(get('db.api'))

    local connopts = get('db.connopts')
    if (get('db.db') == nil) and (type(connopts) == 'table' and connopts.db == nil) then
      error(modname .. ": missing database name parameter")
    end
    if type(connopts) ~= 'table' then
      connopts = {}
      -- Traditional connection parameters
      connopts.host, connopts.user, connopts.port, connopts.pass, connopts.db =
        get('db.host') or 'localhost', get('db.user'), get('db.port'), get('db.pass'), get('db.db')
    end
    connopts.charset = 'utf8'
    connopts.options = connopts.options or {}
    connopts.options.MYSQL_OPT_RECONNECT = true
    conn = mysql.connect(connopts)
    thismod.conn = conn

    -- LuaPower's MySQL interface throws an error when the connection fails, no need to check if
    -- it succeeded.

    -- Ensure UTF-8 is in use.
    -- If you use another encoding, kill yourself (unless it's UTF-32).
    conn:query("SET NAMES 'utf8'")
    conn:query("SET CHARACTER SET utf8")
    conn:query("SET character_set_results = 'utf8', character_set_client = 'utf8'," ..
                   "character_set_connection = 'utf8', character_set_database = 'utf8'," ..
                   "character_set_server = 'utf8'")

    local set = function(setting, val) conn:query('SET ' .. setting .. '=' .. val) end
    pcall(set, 'wait_timeout', 3600)
    pcall(set, 'autocommit', 1)
    pcall(set, 'max_allowed_packet', 67108864)
  end

  local tables = {}
  do -- Tables and schema settings
    local t_auths = get('db.tables.auths')
    if type(t_auths) == 'table' then
      tables.auths = t_auths
    else
      tables.auths = {}
      tables.auths.name = get('db.tables.auths.name')
      tables.auths.schema = {}
      local S = tables.auths.schema
      S.userid = get('db.tables.auths.schema.userid')
      S.username = get('db.tables.auths.schema.username')
      S.password = get('db.tables.auths.schema.password')
      S.privs = get('db.tables.auths.schema.privs')
      S.lastlogin = get('db.tables.auths.schema.lastlogin')
      S.userid_type = get('db.tables.auths.schema.userid_type')
      S.username_type = get('db.tables.auths.schema.username_type')
      S.password_type = get('db.tables.auths.schema.password_type')
      S.privs_type = get('db.tables.auths.schema.privs_type')
      S.lastlogin_type = get('db.tables.auths.schema.lastlogin_type')
    end

    do -- Default values
      tables.auths.name = tables.auths.name or 'auths'
      tables.auths.schema = tables.auths.schema or {}
      local S = tables.auths.schema
      S.userid = S.userid or 'userid'
      S.username = S.username or 'username'
      S.password = S.password or 'password'
      S.privs = S.privs or 'privs'
      S.lastlogin = S.lastlogin or 'lastlogin'

      S.userid_type = S.userid_type or 'INT'
      S.username_type = S.username_type or 'VARCHAR(32)'
      S.password_type = S.password_type or 'VARCHAR(512)'
      S.privs_type = S.privs_type or 'VARCHAR(512)'
      S.lastlogin_type = S.lastlogin_type or 'BIGINT'
      -- Note lastlogin doesn't use the TIMESTAMP type, which is 32-bit and therefore
      -- subject to the year 2038 problem.
    end
  end

  do -- Auth table existence check and setup
    conn:query("SHOW TABLES LIKE '" .. tables.auths.name .. "'")
    local res = conn:store_result()
    local exists = (res:row_count() ~= 0)
    res:free()
    if not exists then
      -- Auth table doesn't exist, create it
      local S = tables.auths.schema
      conn:query('CREATE TABLE ' .. tables.auths.name .. ' (' ..
        S.userid  .. ' ' .. S.userid_type .. ' NOT NULL AUTO_INCREMENT,' ..
        S.username .. ' ' .. S.username_type .. ' NOT NULL,' ..
        S.password .. ' ' .. S.password_type .. ' NOT NULL,' ..
        S.privs .. ' ' .. S.privs_type .. ' NOT NULL,' ..
        S.lastlogin .. ' ' .. S.lastlogin_type .. ',' ..
        'PRIMARY KEY (' .. S.userid .. '),' ..
        'UNIQUE (' .. S.username .. ')' ..
      ')')
    end
  end

  local S = tables.auths.schema
  local get_auth_stmt = conn:prepare('SELECT ' .. S.password .. ',' .. S.privs .. ',' ..
    S.lastlogin .. ' FROM ' .. tables.auths.name .. ' WHERE ' .. S.username .. '=?')
  thismod.get_auth_stmt = get_auth_stmt
  local get_auth_params = get_auth_stmt:bind_params({S.username_type})
  thismod.get_auth_params = get_auth_params
  local get_auth_results = get_auth_stmt:bind_result({S.password_type, S.privs_type,
    S.lastlogin_type})
  thismod.get_auth_results = get_auth_results

  local create_auth_stmt = conn:prepare('INSERT INTO ' .. tables.auths.name .. '(' .. S.username ..
    ',' .. S.password .. ',' .. S.privs .. ') VALUES (?,?,?)')
  thismod.create_auth_stmt = create_auth_stmt
  local create_auth_params = create_auth_stmt:bind_params({S.username_type, S.password_type,
    S.privs_type})
  thismod.create_auth_params = create_auth_params

  local set_password_stmt = conn:prepare('UPDATE ' .. tables.auths.name .. ' SET ' .. S.password ..
    '=? WHERE ' .. S.username .. '=?')
  thismod.set_password_stmt = set_password_stmt
  local set_password_params = set_password_stmt:bind_params({S.password_type, S.username_type})
  thismod.set_password_params = set_password_params

  local set_privileges_stmt = conn:prepare('UPDATE ' .. tables.auths.name .. ' SET ' .. S.privs ..
      '=? WHERE ' .. S.username .. '=?')
  thismod.set_privileges_stmt = set_privileges_stmt
  local set_privileges_params = set_privileges_stmt:bind_params({S.privs_type, S.username_type})
  thismod.set_privileges_params = set_privileges_params

  local record_login_stmt = conn:prepare('UPDATE ' .. tables.auths.name .. ' SET ' ..
    S.lastlogin .. '=? WHERE ' .. S.username .. '=?')
  thismod.record_login_stmt = record_login_stmt
  local record_login_params = record_login_stmt:bind_params({S.lastlogin_type, S.username_type})
  thismod.record_login_params = record_login_params

  thismod.auth_handler = {
    get_auth = function(name)
      assert(type(name) == 'string')
      get_auth_params:set(1, name)
      local success, msg = pcall(get_auth_stmt.exec, get_auth_stmt)
      if not success then
        minetest.log('error', modname .. ": get_auth failed: " .. msg)
        return nil
      end
      get_auth_stmt:store_result()
      if not get_auth_stmt:fetch() then
        -- No such auth row exists
        return nil
      end
      while get_auth_stmt:fetch() do
        minetest.log('warning', modname .. ": get_auth: multiples lines were returned for '" ..
          name .. "'")
      end
      local password, privs_str, lastlogin = get_auth_results:get(1), get_auth_results:get(2),
        get_auth_results:get(3)
      local admin = (name == minetest.setting_get("name"))
      local privs
      if singleplayer or admin then
        privs = {}
        -- If admin, grant all privs, if singleplayer, grant all privs w/ give_to_singleplayer
        for priv, def in pairs(core.registered_privileges) do
          if (singleplayer and def.give_to_singleplayer) or admin then
            privs[priv] = true
          end
        end
      else
        privs = minetest.string_to_privs(privs_str)
      end
      return {
        password = password,
        privileges = privs,
        last_login = lastlogin
      }
    end,
    create_auth = function(name, password, reason)
      assert(type(name) == 'string')
      assert(type(password) == 'string')
      minetest.log('info', modname .. " creating player '"..name.."'" .. (reason or ""))
      create_auth_params:set(1, name)
      create_auth_params:set(2, password)
      create_auth_params:set(3, minetest.setting_get("default_privs"))
      local success, msg = pcall(create_auth_stmt.exec, create_auth_stmt)
      if not success then
        minetest.log('error', modname .. ": create_auth failed: " .. msg)
        return false
      end
      if create_auth_stmt:affected_rows() ~= 1 then
        minetest.log('error', modname .. ": create_auth failed: affected row count is " ..
          create_auth_stmt:affected_rows() .. ", expected 1")
        return false
      end
      return true
    end,
    set_password = function(name, password)
      assert(type(name) == 'string')
      assert(type(password) == 'string')
      if not thismod.auth_handler.get_auth(name) then
        return thismod.auth_handler.create_auth(name, password, " because set_password was requested")
      else
        minetest.log('info', modname .. " setting password of player '" .. name .. "'")
        set_password_params:set(1, password)
        set_password_params:set(2, name)
        local success, msg = pcall(set_password_stmt.exec, set_password_stmt)
        if not success then
          minetest.log('error', modname .. ": set_password failed: " .. msg)
          return false
        end
        if set_password_stmt:affected_rows() ~= 1 then
          minetest.log('error', modname .. ": set_password failed: affected row count is " ..
            set_password_stmt:affected_rows() .. ", expected 1")
          return false
        end
        return true
      end
    end,
    set_privileges = function(name, privileges)
      assert(type(name) == 'string')
      assert(type(privileges) == 'table')
      set_privileges_params:set(1, minetest.privs_to_string(privileges))
      set_privileges_params:set(2, name)
      local success, msg = pcall(set_privileges_stmt.exec, set_privileges_stmt)
      if not success then
        minetest.log('error', modname .. ": set_privileges failed: " .. msg)
        return false
      end
      minetest.notify_authentication_modified(name)
      if set_privileges_stmt:affected_rows() ~= 1 then
        minetest.log('error', modname .. ": set_privileges failed: affected row count is " ..
          set_privileges_stmt:affected_rows() .. ", expected 1")
        return false
      end
      return true
    end,
    reload = function()
      return true
    end,
    record_login = function(name)
      assert(type(name) == 'string')
      record_login_params:set(1, math.floor(os.time()))
      record_login_params:set(2, name)
      local success, msg = pcall(record_login_stmt.exec, record_login_stmt)
      if not success then
        minetest.log('error', modname .. ": record_login failed: " .. msg)
        return false
      end
      if record_login_stmt:affected_rows() ~= 1 then
        minetest.log('error', modname .. ": record_login failed: affected row count is " ..
          record_login_stmt:affected_rows() .. ", expected 1")
        return false
      end
      return true
    end
  }
end

minetest.register_authentication_handler(thismod.auth_handler)
minetest.log('action', modname .. ": Registered auth handler")

local function ping()
  if thismod.conn then
    if not thismod.conn:ping() then
      minetest.log('error', modname .. ": failed to ping database")
    end
  end
  minetest.after(1800, ping)
end
minetest.after(10, ping)

minetest.register_on_shutdown(function()
  if thismod.conn then
    thismod.get_auth_stmt:free_result()
    thismod.conn:close()
    thismod.conn = nil
  end
end)
