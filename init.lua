local modname = minetest.get_current_modname()
local modpath = minetest.get_modpath(modname) 

local thismod = {
  enabled = false
}
_G[modname] = thismod

if not mysql_base.enabled then
  minetest.log('action', modname .. ": mysql_base disabled, not loading mod")
  return
end

local singleplayer = minetest.is_singleplayer() -- Caching is OK since you can't open a game to
-- multiplayer unless you restart it.
if not minetest.setting_get(modname .. '.enable_singleplayer') and singleplayer then
  minetest.log('action', modname .. ": Not adding auth handler because of singleplayer game")
  return
end

enabled = true

do
  local get = mysql_base.mkget(modname)

  local conn, dbname = mysql_base.conn, mysql_base.dbname

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

  local auth_table_created
  -- Auth table existence check and setup
  if not mysql_base.table_exists(tables.auths.name) then
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
    minetest.log('action', modname .. " created table '" .. dbname .. "." .. tables.auths.name ..
      "'")
    auth_table_created = true
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
    ',' .. S.password .. ',' .. S.privs .. ',' .. S.lastlogin .. ') VALUES (?,?,?,?)')
  thismod.create_auth_stmt = create_auth_stmt
  local create_auth_params = create_auth_stmt:bind_params({S.username_type, S.password_type,
    S.privs_type, S.lastlogin_type})
  thismod.create_auth_params = create_auth_params

  local delete_auth_stmt = conn:prepare('DELETE FROM ' .. tables.auths.name .. ' WHERE ' ..
    S.username .. '=?')
  thismod.delete_auth_stmt = delete_auth_stmt
  local delete_auth_params = delete_auth_stmt:bind_params({S.username_type})
  thismod.delete_auth_params = delete_auth_params

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

  local enumerate_auths_query = 'SELECT ' .. S.username .. ',' .. S.password .. ',' .. S.privs ..
    ',' .. S.lastlogin .. ' FROM ' .. tables.auths.name
  thismod.enumerate_auths_query = enumerate_auths_query

  if auth_table_created and get('import_auth_txt_on_table_create') ~= 'false' then
    if not thismod.import_auth_txt then
      dofile(modpath .. '/auth_txt_import.lua')
    end
    thismod.import_auth_txt()
  end

  thismod.auth_handler = {
    get_auth = function(name)
      assert(type(name) == 'string')
      get_auth_params:set(1, name)
      local success, msg = pcall(get_auth_stmt.exec, get_auth_stmt)
      if not success then
        minetest.log('error', modname .. ": get_auth(" .. name .. ") failed: " .. msg)
        return nil
      end
      get_auth_stmt:store_result()
      if not get_auth_stmt:fetch() then
        -- No such auth row exists
        return nil
      end
      while get_auth_stmt:fetch() do
        minetest.log('warning', modname .. ": get_auth(" .. name .. "): multiples lines were" ..
          " returned")
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
        if admin and not thismod.admin_get_auth_called then
          thismod.admin_get_auth_called = true
          thismod.auth_handler.set_privileges(name, privs)
        end
      else
        privs = minetest.string_to_privs(privs_str)
      end
      return {
        password = password,
        privileges = privs,
        last_login = tonumber(lastlogin)
      }
    end,
    create_auth = function(name, password, reason)
      assert(type(name) == 'string')
      assert(type(password) == 'string')
      minetest.log('info', modname .. " creating player '"..name.."'" .. (reason or ""))
      create_auth_params:set(1, name)
      create_auth_params:set(2, password)
      create_auth_params:set(3, minetest.setting_get("default_privs"))
      create_auth_params:set(4, math.floor(os.time()))
      local success, msg = pcall(create_auth_stmt.exec, create_auth_stmt)
      if not success then
        minetest.log('error', modname .. ": create_auth(" .. name .. ") failed: " .. msg)
        return false
      end
      if create_auth_stmt:affected_rows() ~= 1 then
        minetest.log('error', modname .. ": create_auth(" .. name .. ") failed: affected row" ..
          " count is " .. create_auth_stmt:affected_rows() .. ", expected 1")
        return false
      end
      return true
    end,
    delete_auth = function(name)
      assert(type(name) == 'string')
      minetest.log('info', modname .. " deleting player '"..name.."'")
      delete_auth_params:set(1, name)
      local success, msg = pcall(delete_auth_stmt.exec, delete_auth_stmt)
      if not success then
        minetest.log('error', modname .. ": delete_auth(" .. name .. ") failed: " .. msg)
        return false
      end
      if delete_auth_stmt:affected_rows() ~= 1 then
        minetest.log('error', modname .. ": delete_auth(" .. name .. ") failed: affected row" ..
          " count is " .. delete_auth_stmt:affected_rows() .. ", expected 1")
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
          minetest.log('error', modname .. ": set_password(" .. name .. ") failed: " .. msg)
          return false
        end
        if set_password_stmt:affected_rows() ~= 1 then
          minetest.log('error', modname .. ": set_password(" .. name .. ") failed: affected row" ..
            " count is " .. set_password_stmt:affected_rows() .. ", expected 1")
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
        minetest.log('error', modname .. ": set_privileges(" .. name .. ") failed: " .. msg)
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
      local success, msg = pcall(record_login_stmt.exec, record_login_stmt)
      if not success then
        minetest.log('error', modname .. ": record_login(" .. name .. ") failed: " .. msg)
        return false
      end
      if record_login_stmt:affected_rows() ~= 1 then
        minetest.log('error', modname .. ": record_login(" .. name .. ") failed: affected row" ..
          " count is " .. record_login_stmt:affected_rows() .. ", expected 1")
        return false
      end
      return true
    end,
    enumerate_auths = function()
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
minetest.log('action', modname .. ": Registered auth handler")

mysql_base.register_on_shutdown(function()
  thismod.get_auth_stmt:free_result()
end)
