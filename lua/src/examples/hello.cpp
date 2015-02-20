#include <hpx/hpx_main.hpp>
#include <xlua.hpp>

/**
 * This program provides an
 * example of how to call
 * Lua from within an hpx
 * program.
 */

int main() {

  // Access to LUA in HPX is managed
  // through the hpx::LuaEnv class.
  // This should ideally be short lived.
  // Use in place of the lua_State* type.
  hpx::LuaEnv lenv;

  // Pass values in through globals
  lua_pushnumber(lenv,666);
  lua_setglobal(lenv,"arg");

  // Call some bit of lua code...
  int rc = luaL_dostring(lenv,

    // Define functions to be used by hpx
    "function pr(arg) "
    "  here = locality.find_here() "
    "  print('hello from '..here..' value = '..arg)"
    "end "
    "function do_stuff(a) "
    "  all = locality.all_localities() "
    "  for k,v in ipairs(all) do "
    "    print('k=',k,'v=',''..v) "
    "    async(v,'pr',k)"
    "  end "
    "  return a+1,'done' "
    "end "
    
    // Register functions
    "hpx_reg('pr','do_stuff') "
    );

  // Call a function. First, get the function onto the stack
  lua_getglobal(lenv,"do_stuff");

  // Now push an argument onto the stack
  lua_pushnumber(lenv,666);

  // Now call pcall
  int nargs = 1;
  int results = 2;
  int errfunc = 0;
  rc = lua_pcall(lenv,nargs,results,errfunc);

  // report errors if any...
  if(rc != 0) {
    std::cout << "Lua Error: " << lua_tostring(lenv,-1) << std::endl;

  // extract return values if any...
  } else {
    int n = lua_gettop(lenv);
    for(int i=1;i<=n;i++) {
      if(lua_isnumber(lenv,i)) {
        std::cout << "lua[" << i << "]=" << lua_tonumber(lenv,i) << std::endl;
      } else if(lua_isstring(lenv,i)) {
        std::cout << "lua[" << i << "]=" << lua_tostring(lenv,i) << std::endl;
      }
    }
  }

  return 0;
}
