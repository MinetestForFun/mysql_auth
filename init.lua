local modname = minetest.get_current_modname()
local modpath = minetest.get_modpath(modname) 

local thismod = {
  enabled = false
}
_G[modname] = thismod

local LogI = mysql_base.mklog('action', modname)
local LogE = mysql_base.mklog('error', modname)

if not mysql_base.enabled then
  LogI("mysql_base disabled, not loading mod")
  return
end

local singleplayer = minetest.is_singleplayer() -- Caching is OK since you can't open a game to
-- multiplayer unless you restart it.
if not minetest.settings:get(modname .. '.enable_singleplayer') and singleplayer then
  LogI("Not adding auth handler because of singleplayer game")
  return
end

thismod.enabled = true

local LogV = function() end
do
  local get = mysql_base.mkget(modname)
  if get('verbose') == 'true' then
    LogI("Verbose logging enabled")
    LogV = mysql_base.mklog('verbose', modname)
  end

  local conn, dbname = mysql_base.conn, mysql_base.dbname

  local tables = {}
  thismod.tables = tables
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

  local auth_table_created
  -- Auth table existence check and setup
  if not mysql_base.table_exists(tables.auths.name) then
    -- Auth table doesn't exist, create it
    local S = tables.auths.schema
    mysql_base.create_table(tables.auths.name, {
      columns = {
        {S.userid, S.userid_type, notnull = true, autoincrement = true},
        {S.username, S.username_type, notnull = true},
        {S.password, S.password_type, notnull = true},
        {S.privs, S.privs_type, notnull = true},
        {S.lastlogin, S.lastlogin_type},
      },
      pkey = {S.userid},
      unique = {S.username},
    })
    LogI("Created table '" .. dbname .. "." .. tables.auths.name .. "'")
    auth_table_created = true
  end

  local S = tables.auths.schema
  local get_auth_stmt, get_auth_params, get_auth_results = mysql_base.prepare_select(
    tables.auths.name, {
    {S.userid, S.userid_type},
    {S.password, S.password_type},
    {S.privs, S.privs_type},
    {S.lastlogin, S.lastlogin_type}},
    S.username .. '=?', {S.username_type})
  thismod.get_auth_stmt = get_auth_stmt

  local create_auth_stmt, create_auth_params = mysql_base.prepare_insert(
    tables.auths.name, {
    {S.username, S.username_type},
    {S.password, S.password_type},
    {S.privs, S.privs_type},
    {S.lastlogin, S.lastlogin_type},
  })
  thismod.create_auth_stmt = create_auth_stmt
  thismod.create_auth_params = create_auth_params
  local max_name_len = tonumber(create_auth_params.buffer[0].buffer_length)
  local max_pass_len = tonumber(create_auth_params.buffer[1].buffer_length)

  local delete_auth_stmt, delete_auth_params = mysql_base.prepare_delete(tables.auths.name,
    S.username .. '=?', {S.username_type})

  local set_password_stmt, set_password_params = mysql_base.prepare_update(tables.auths.name,
    {{S.password, S.password_type}},
    S.username .. '=?', {S.username_type})

  local set_privileges_stmt, set_privileges_params = mysql_base.prepare_update(tables.auths.name,
    {{S.privs, S.privs_type}},
    S.username .. '=?', {S.username_type})
  local max_privs_len = tonumber(set_privileges_params.buffer[0].buffer_length)

  local record_login_stmt, record_login_params = mysql_base.prepare_update(tables.auths.name,
    {{S.lastlogin, S.lastlogin_type}},
    S.username .. '=?', {S.username_type})

  local enumerate_auths_query = 'SELECT ' .. S.username .. ',' .. S.password .. ',' .. S.privs ..
    ',' .. S.lastlogin .. ' FROM ' .. tables.auths.name
  thismod.enumerate_auths_query = enumerate_auths_query

  if auth_table_created and get('import_auth_txt_on_table_create') == 'true' then
    if not thismod.import_auth_txt then
      dofile(modpath .. '/auth_txt_import.lua')
    end
    thismod.import_auth_txt()
  end

  thismod.auth_handler = {
    get_auth = function(name)
      assert(type(name) == 'string')
      if name:len() > max_name_len then
        LogE("get_auth(" .. name .. ") failed: name too long (max " .. max_name_len .. ")")
        return nil
      end
      get_auth_params:set(1, name)
      local success, msg = pcall(get_auth_stmt.exec, get_auth_stmt)
      if not success then
        LogE("get_auth(" .. name .. ") failed: " .. msg)
        return nil
      end
      get_auth_stmt:store_result()
      if not get_auth_stmt:fetch() then
        -- No such auth row exists
        return nil
      end
      while get_auth_stmt:fetch() do
        error(modname .. ": get_auth(" .. name .. "): multiples lines were returned")
      end
      local userid, password, privs_str, lastlogin = get_auth_results:get(1),
        get_auth_results:get(2), get_auth_results:get(3), get_auth_results:get(4)
      local admin
      if minetest.settings then
        admin = (name == minetest.settings:get("name"))
      else
        admin = (name == minetest.setting_get("name"))
      end
      local privs
      if singleplayer or admin then
        privs = {}
        -- If admin, grant all privs, if singleplayer, grant all privs w/ give_to_singleplayer
        for priv, def in pairs(core.registered_privileges) do
          if (singleplayer and def.give_to_singleplayer) or admin then
            privs[priv] = true
          end
        end
        if admin and not thismod.admin_get_auth_called then
          thismod.admin_get_auth_called = true
          thismod.auth_handler.set_privileges(name, privs)
        end
      else
        privs = minetest.string_to_privs(privs_str)
      end
      LogV("get_auth(" .. name .. ") -> {userid:" .. userid .. ", privileges: " ..
           table.concat(privs, ',') .. "}")
      return {
        userid = userid,
        password = password,
        privileges = privs,
        last_login = tonumber(lastlogin)
      }
    end,
    create_auth = function(name, password, reason)
      assert(type(name) == 'string')
      assert(type(password) == 'string')
      LogV("create_auth(" .. name .. ", ###" .. (reason and (", " .. reason) or "") .. ")")
      LogI("Creating player '" .. name .. "'" .. (reason or ""))
      if name:len() > max_name_len then
        LogE("create_auth(" .. name .. ") failed: name too long (max " .. max_name_len .. ")")
        return false
      end
      if password:len() > max_pass_len then
        LogE("create_auth(" .. name .. ") failed: password too long (max " .. max_pass_len .. ")")
        return false
      end
      create_auth_params:set(1, name)
      create_auth_params:set(2, password)
      if minetest.settings then
        create_auth_params:set(3, minetest.settings:get("default_privs"))
      else
        create_auth_params:set(3, minetest.setting_get("default_privs"))
      end
      create_auth_params:set(4, math.floor(os.time()))
      local success, msg = pcall(create_auth_stmt.exec, create_auth_stmt)
      if not success then
        LogE("create_auth(" .. name .. ") failed: " .. msg)
        return false
      end
      if create_auth_stmt:affected_rows() ~= 1 then
        LogE("create_auth(" .. name .. ") failed: affected row count is " ..
             create_auth_stmt:affected_rows() .. ", expected 1")
        return false
      end
      return true
    end,
    delete_auth = function(name)
      assert(type(name) == 'string')
      LogV("delete_auth(" .. name .. ")")
      LogI("Deleting player '"..name.."'")
      if name:len() > max_name_len then
        LogE("delete_auth(" .. name .. ") failed: name too long (max " .. max_name_len .. ")")
        return false
      end
      delete_auth_params:set(1, name)
      local success, msg = pcall(delete_auth_stmt.exec, delete_auth_stmt)
      if not success then
        LogE("delete_auth(" .. name .. ") failed: " .. msg)
        return false
      end
      if delete_auth_stmt:affected_rows() ~= 1 then
        LogE("delete_auth(" .. name .. ") failed: affected row count is " ..
             delete_auth_stmt:affected_rows() .. ", expected 1")
        return false
      end
      return true
    end,
    set_password = function(name, password)
      assert(type(name) == 'string')
      assert(type(password) == 'string')
      LogV("set_password(" .. name .. ", ###)")
      if name:len() > max_name_len then
        LogE("create_auth(" .. name .. ") failed: name too long (max " .. max_name_len .. ")")
        return false
      end
      if password:len() > max_pass_len then
        LogE("create_auth(" .. name .. ") failed: password too long (max " .. max_pass_len .. ")")
        return false
      end
      if not thismod.auth_handler.get_auth(name) then
        return thismod.auth_handler.create_auth(name, password, " because set_password was requested")
      else
        LogI("Setting password of player '" .. name .. "'")
        set_password_params:set(1, password)
        set_password_params:set(2, name)
        local success, msg = pcall(set_password_stmt.exec, set_password_stmt)
        if not success then
          LogE("set_password(" .. name .. ") failed: " .. msg)
          return false
        end
        if set_password_stmt:affected_rows() ~= 1 then
          LogE("set_password(" .. name .. ") failed: affected row  count is " ..
               set_password_stmt:affected_rows() .. ", expected 1")
          return false
        end
        return true
      end
    end,
    set_privileges = function(name, privileges)
      assert(type(name) == 'string')
      assert(type(privileges) == 'table')
      local privstr = minetest.privs_to_string(privileges)
      LogV("set_privileges(" .. name .. ", {" .. table.concat(privileges, ', ') .. "}) [" ..
           privstr .. "]")
      if name:len() > max_name_len then
        LogE("set_privileges(" .. name .. ") failed: name too long (max " .. max_name_len .. ")")
        return false
      end
      if privstr:len() > max_privs_len then
        LogE("create_auth(" .. name .. ") failed: priv string too long (max " ..
             max_privs_len .. ")")
        return false
      end
      set_privileges_params:set(1, privstr)
      set_privileges_params:set(2, name)
      local success, msg = pcall(set_privileges_stmt.exec, set_privileges_stmt)
      if not success then
        LogE("set_privileges(" .. name .. ") failed: " .. msg)
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
      LogV("record_login(" .. name .. ")")
      if name:len() > max_name_len then
        LogE("set_privileges(" .. name .. ") failed: name too long (max " .. max_name_len .. ")")
        return false
      end
      record_login_params:set(1, math.floor(os.time()))
      record_login_params:set(2, name)
      local success, msg = pcall(record_login_stmt.exec, record_login_stmt)
      if not success then
        LogE("record_login(" .. name .. ") failed: " .. msg)
        return false
      end
      if record_login_stmt:affected_rows() ~= 1 then
        LogE("record_login(" .. name .. ") failed: affected row count is " ..
             record_login_stmt:affected_rows() .. ", expected 1")
        return false
      end
      return true
    end,
    enumerate_auths = function()
      LogV("enumerate_auths()")
      conn:query(enumerate_auths_query)
      local res = conn:store_result()
      return function()
        local row = res:fetch('n')
        if not row then
          return nil
        end
        local username, password, privs_str, lastlogin = unpack(row)
        return username, {
          password = password,
          privileges = minetest.string_to_privs(privs_str),
          last_login = tonumber(lastlogin)
        }
      end
    end
  }
end

minetest.register_authentication_handler(thismod.auth_handler)
LogI("Registered auth handler")

mysql_base.register_on_shutdown(function()
  thismod.get_auth_stmt:free_result()
end)
