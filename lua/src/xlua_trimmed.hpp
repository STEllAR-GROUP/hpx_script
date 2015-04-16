namespace hpx {

class Lua;

class LuaEnv {
  Lua *ptr;
  lua_State *L;
public:
  lua_State *get_state();
  LuaEnv();
  ~LuaEnv();
  operator lua_State *() {
    return L;
  }
};

}
