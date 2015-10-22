#include "xlua.hpp"
#include "xlua_prototypes.hpp"
#include <hpx/util/apex.hpp>
#include <set>

namespace hpx {

const int max_output_args = 10;

#define INSERT(X) } else if(s == #X) { when.insert(X); 
#define CASE(X) case X: lua_pushstring(L,#X); break;

int apex_register_policy(lua_State *L) {
  int n = lua_gettop(L);
  Bytecode b;
  lua_pushvalue(L,1);
  lua_dump(L,(lua_Writer)lua_write,&b.data);
  lua_pop(L,1);
  std::set<apex_event_type> when;
  for(int i=2;i<=n;i++) {
    if(lua_isstring(L,i)) {
      std::string s = lua_tostring(L,i);
      if(false) {
      INSERT(APEX_STARTUP)
      INSERT(APEX_SHUTDOWN)
      INSERT(APEX_NEW_NODE)
      INSERT(APEX_NEW_THREAD)
      INSERT(APEX_START_EVENT)
      INSERT(APEX_STOP_EVENT)
      INSERT(APEX_SAMPLE_VALUE)
      } else {
        std::cout << "Unknown apex policy" << std::endl;
        return 0;
      }
    }
  }
    //std::set<apex_event_type> when = {APEX_STARTUP, APEX_SHUTDOWN, APEX_NEW_NODE,
        //APEX_NEW_THREAD, APEX_START_EVENT, APEX_STOP_EVENT, APEX_SAMPLE_VALUE};
  apex::register_policy(when, [b](apex_context const& context)->int{
    LuaEnv lenv;
    lua_State *L = lenv;
    lua_pop(L,lua_gettop(L));
    lua_load(L,(lua_Reader)lua_read,(void *)&b.data,0,"b");
    std::string str;
    switch(context.event_type) {
      CASE(APEX_STARTUP)
      CASE(APEX_SHUTDOWN)
      CASE(APEX_NEW_NODE)
      CASE(APEX_NEW_THREAD)
      CASE(APEX_START_EVENT)
      CASE(APEX_STOP_EVENT)
      CASE(APEX_SAMPLE_VALUE)
      default:
        std::cout << "Unknown apex event type" << std::endl;
    }
    if(lua_pcall(L,lua_gettop(L)-1,max_output_args,0) != 0) {
      SHOW_ERROR(L);
    }
    return APEX_NOERROR;
  });
  return 0;
}

}
