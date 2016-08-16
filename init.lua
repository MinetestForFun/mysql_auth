local modname = minetest.get_current_modname()
local modpath = minetest.get_modpath(modname) 

local thismod = {}
_G[modname] = thismod

if not minetest.setting_get(modname .. '.enable_singleplayer') and minetest.is_singleplayer() then
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
      local cfg = minetest.deserialize(file:read('*a'))
      file:close()
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

  local conn, db
  do
    -- MySQL API backend
    mysql.config(get('db.api'))

    local connopts = get('db.connopts')
    if type(connopts) == 'table' then
      -- User-specified connection parameter table
      -- Only when using a config file
      db = connopts.db
      connopts.charset = 'utf8'
      conn = mysql.connect(connopts)
    elseif get('db.db') ~= nil then
      -- Traditional connection parameters
      local host, user, port = get('db.host') or 'localhost', get('db.user'), get('db.port')
      local pass = get('db.pass')
      db = get('db.db')
      conn = mysql.connect(host, user, pass, db, 'utf8', port)
    else
      error(modname .. ": missing db.db parameter")
    end
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
  local get_auth_params = get_auth_stmt:bind_params({S.username_type})
  local get_auth_results = get_auth_stmt:bind_result({S.password_type, S.privs_type,
    S.lastlogin_type})

  local create_auth_stmt = conn:prepare('INSERT INTO ' .. tables.auths.name .. '(' .. S.username ..
    ',' .. S.password .. ',' .. S.privs .. ') VALUES (?,?,?)')
  local create_auth_params = create_auth_stmt:bind_params({S.username_type, S.password_type,
    S.privs_type})

  local set_password_stmt = conn:prepare('UPDATE ' .. tables.auths.name .. ' SET ' .. S.password ..
    '=? WHERE ' .. S.username .. '=?')
  local set_password_params = set_password_stmt:bind_params({S.password_type, S.username_type})

  local set_privileges_stmt = conn:prepare('UPDATE ' .. tables.auths.name .. ' SET ' .. S.privs ..
      '=? WHERE ' .. S.username .. '=?')
  local set_privileges_params = set_privileges_stmt:bind_params({S.privs_type, S.username_type})

  local record_login_stmt = conn:prepare('UPDATE ' .. tables.auths.name .. ' SET ' ..
    S.lastlogin .. '=? WHERE ' .. S.username .. '=?')
  local record_login_params = record_login_stmt:bind_params({S.lastlogin_type, S.username_type})

  thismod.auth_handler = {
    get_auth = function(name)
      assert(type(name) == 'string')
      get_auth_params:set(1, name)
      local success, msg = pcall(function () get_auth_stmt:exec() end)
      if not success then
        minetest.log('error', modname .. ': get_auth failed: ' .. msg)
        return nil
      end
      if not get_auth_stmt:fetch() then
        minetest.log('error', modname .. ': get_auth failed: get_auth_stmt:fetch() returned false')
        return nil
      end
      local password, privs_str, lastlogin = get_auth_results:get(1), get_auth_results:get(2),
        get_auth_results:get(3)
      get_auth_stmt:free_result()
      return {
        password = password,
        privileges = minetest.string_to_privs(privs_str),
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
      local success, msg = pcall(function () create_auth_stmt:exec() end)
      if not success then
        minetest.log('error', modname .. ': create_auth failed: ' .. msg)
        return false
      end
      return true
    end,
    set_password = function(name, password)
      assert(type(name) == 'string')
      assert(type(password) == 'string')
      if not thismod.auth_handler.get_auth(name) then
        thismod.auth_handler.create_auth(name, password, ' because set_password was requested')
      else
        minetest.log('info', modname .. " setting password of player '"..name.."'")
        set_password_params:set(1, password)
        set_password_params:set(2, name)
        local success, msg = pcall(function () set_password_stmt:exec() end)
        if not success then
          minetest.log('error', modname .. ': set_password failed: ' .. msg)
          return false
        end
      end
      return true
    end,
    set_privileges = function(name, privileges)
      assert(type(name) == 'string')
      assert(type(privileges) == 'table')
      set_privileges_params:set(1, minetest.privs_to_string(privileges))
      set_privileges_params:set(2, name)
      local success, msg = pcall(function () set_privileges_stmt:exec() end)
      if not success then
        minetest.log('error', modname .. ': set_privileges failed: ' .. msg)
        return false
      end
      minetest.notify_authentication_modified(name)
      return true
    end,
    reload = function()
      return true
    end,
    record_login = function(name)
      assert(type(name) == 'string')
      record_login_params:set(1, math.floor(os.time()))
      record_login_params:set(2, name)
      local success, msg = pcall(function () record_login_stmt:exec() end)
      if not success then
        minetest.log('error', modname .. ': record_login failed: ' .. msg)
        return false
      end
      return true
    end
  }
end

minetest.register_authentication_handler(thismod.auth_handler)
minetest.log('action', modname .. ": Registered auth handler")

minetest.register_on_shutdown(function()
  if thismod.conn then
    thismod.conn:close()
  end
end)
