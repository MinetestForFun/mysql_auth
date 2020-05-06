local modname = minetest.get_current_modname()
local thismod = _G[modname]

function thismod.import_auth_txt()
  minetest.log('action', modname .. ": Importing auth.txt")
  local auth_file_path = minetest.get_worldpath() .. '/auth.txt'
  local create_auth_stmt = thismod.create_auth_stmt
  local create_auth_params = thismod.create_auth_params
  local conn = mysql_base.conn
  local file, errmsg = io.open(auth_file_path, 'rb')
  if not file then
    minetest.log('action', modname .. ": " .. auth_file_path .. " could not be opened for reading" ..
       "(" .. errmsg .. "); no auth entries imported")
    return
  end
  conn:query('SET autocommit=0')
  conn:query('START TRANSACTION')
  for line in file:lines() do
    if line ~= "" then
      local fields = line:split(":", true)
      local name, password, privilege_string, last_login = unpack(fields)
      last_login = tonumber(last_login)
      if not (name and password and privilege_string) then
        minetest.log('warning', modname .. ": Invalid line in auth.txt, skipped: " .. dump(line))
      end
      minetest.log('info', modname .. " importing player '"..name.."'")
      create_auth_params:set(1, name)
      create_auth_params:set(2, password)
      create_auth_params:set(3, privilege_string)
      create_auth_params:set(4, last_login)
      local success, msg = pcall(create_auth_stmt.exec, create_auth_stmt)
      if not success then
        error(modname .. ": import failed: " .. msg)
      end
      if create_auth_stmt:affected_rows() ~= 1 then
        error(modname .. ": create_auth failed: affected row count is " ..
          create_auth_stmt:affected_rows() .. ", expected 1")
      end
    end
  end
  conn:query('COMMIT')
  conn:query('SET autocommit=1')
  io.close(file)
  minetest.log('action', modname .. ": Finished importing auth.txt")
end
