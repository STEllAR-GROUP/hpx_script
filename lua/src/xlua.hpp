#ifndef XLUA_HPP
#define XLUA_HPP

#include <iostream>
#include <hpx/hpx.hpp>
#include <hpx/hpx_init.hpp>
#include <lua.hpp>
#include <map>
#include <sstream>
#include <hpx/include/lcos.hpp>
#include <atomic>
#include <memory>
#include <sstream>
#include <boost/bind.hpp>
#include <boost/variant.hpp>
#include <hpx/lcos/future.hpp>
#include <hpx/lcos/local/composable_guard.hpp>
#include <hpx/include/actions.hpp>
#include <hpx/runtime/serialization/shared_ptr.hpp>
#include <hpx/runtime/serialization/map.hpp>
#include <hpx/runtime/serialization/vector.hpp>
#include <hpx/runtime/serialization/variant.hpp>
#include <stdexcept>

#define SHOW_ERROR(L) do { std::cout \
    << "Error: " << __FILE__ << ":" << __LINE__ << " " \
    << lua_tostring(L,-1) << std::endl; lua_pop(L,1); } while(false)

#define HERE std::cout << "HERE: " << __FILE__ << ":" << __LINE__ << std::endl

#define OUT(I,V) do { o << I << "] " << V << std::endl; } while(false)
#define OUT2(I,V,V2) do { o << I << "] " << V << " " << V2 << std::endl; } while(false)

#define STACK show_stack(std::cout,L,__FILE__,__LINE__,true)

namespace hpx {

std::ostream& show_stack(std::ostream& o,lua_State *L,const char *fname,int line,bool recurse=true);

/*
 * This class is present because of a bug in boost (?) which
 * prevents the serialization of shared_ptr<string>.
 */
struct string_wrap {
  std::string str;
  const char *c_str() const { return str.c_str(); }
  operator const std::string&() const { return str; }
  string_wrap() {}
  string_wrap(const char *s) : str(s) {}
  ~string_wrap() {}
private:
    friend class hpx::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
      ar & str;
    }
};
inline std::ostream& operator<<(std::ostream& o,const string_wrap& sw) {
  return o << sw.str;
}

class Holder;
std::ostream& operator<<(std::ostream&,const Holder&);

int dataflow(lua_State *L);
int make_ready_future(lua_State *L);
int async(lua_State *L);
int luax_wait_all(lua_State *L);
int luax_when_all(lua_State *L);
int luax_when_any(lua_State *L);
int unwrap(lua_State *L);
int find_here(lua_State *L);
int all_localities(lua_State *L);
int remote_localities(lua_State *L);
int root_locality(lua_State *L);

int hpx_reg(lua_State *L);

int isfuture(lua_State *L);

int hpx_run(lua_State *L);


int luax_run_guarded(lua_State *L);

int open_future(lua_State *L);
int open_guard(lua_State *L);
int open_locality(lua_State *L);

int new_future(lua_State *L);

const char *lua_read(lua_State *L,void *data,size_t *size);
int lua_write(lua_State *L,const char *str,unsigned long len,std::string *buf);

// metatables
extern const char *future_metatable_name;
extern const char *guard_metatable_name;
extern const char *locality_metatable_name;

typedef hpx::naming::id_type locality_type;

struct Bytecode {
  std::string data;
private:
  friend class hpx::serialization::access;
  template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
      ar & data;
    }
};
 
class Holder;

typedef boost::shared_ptr<std::vector<Holder> > ptr_type;
typedef hpx::shared_future<ptr_type> future_type;
typedef boost::variant<double,std::string> key_type;
typedef std::map<key_type,Holder> table_type;
typedef std::vector<Holder> array_type;

struct Guard {
  boost::shared_ptr<hpx::lcos::local::guard> g;
  ptr_type g_data;
  Guard() : g(new hpx::lcos::local::guard()), g_data(new std::vector<Holder>()) {}
  ~Guard() {}
};
typedef boost::shared_ptr<Guard> guard_type;

typedef boost::shared_ptr<string_wrap> string_ptr;

int hpx_srun(lua_State *L,std::string& fname,ptr_type p);
void hpx_srun(string_ptr fname,ptr_type p,guard_type*,int);

extern guard_type global_guarded;

struct Empty {
private:
    friend class hpx::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
    }
};

//--- Generic holder for a Lua data object
class Holder {
private:
    friend class hpx::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
      ar & var;
    }
public:
  enum utype { empty_t, num_t, fut_t, str_t, ptr_t, table_t, bytecode_t };

  boost::variant<
    Empty,
    double,
    future_type,
    std::string,
    ptr_type,
    table_type,
    Bytecode
    > var;

  void set(double num_) {
    var = num_;
  }
  void set(std::string& str_) {
    var = str_;
  }
  void set(Bytecode& bc_) {
    var = bc_;
  }
  void set(const char *str_) {
    std::string s(str_);
    var = s;
  }
  void unpack(lua_State *L) {
    if(var.which() == num_t) {
      lua_pushnumber(L,boost::get<double>(var));
    } else if(var.which() == str_t) {
      lua_pushstring(L,boost::get<std::string>(var).c_str());
    } else if(var.which() == ptr_t) {
      auto ptr = boost::get<ptr_type >(var);
      for(auto i=ptr->begin();i != ptr->end();++i)
        i->unpack(L);
    } else if(var.which() == fut_t) {
      // Shouldn't ever happen
      //std::cout << "ERROR: Unrealized future in arg list" << std::endl;
      //abort();
      new_future(L);
      future_type *fc = (future_type *)lua_touserdata(L,-1);
      *fc = boost::get<future_type>(var);
    } else if(var.which() == table_t) {
      try {
        table_type& table = boost::get<table_type>(var);
        lua_createtable(L,0,table.size());
        for(auto i=table.begin();i != table.end();++i) {
          const int which = i->first.which();
          if(which == 0) { // number
            double d = boost::get<double>(i->first);
            lua_pushnumber(L,d);
            if(d == 0) {
              std::cout << "unpack:PRINT=" << (*this) << std::endl;
              abort();
            }
          } else if(which == 1) { // string
            std::string str = boost::get<std::string>(i->first);
            lua_pushstring(L,str.c_str());
          } else {
            std::cout << "ERROR: Unknown type: " << which << std::endl;
            abort();
          }
          i->second.unpack(L);
          lua_settable(L,-3);
        }
      } catch(std::exception e) {
        std::cout << "EX2=" << e.what() << std::endl;
      }
    } else if(var.which() == bytecode_t) {
      Bytecode& bc = boost::get<Bytecode>(var);
      lua_load(L,(lua_Reader)lua_read,(void *)&bc.data,"func","b");
    } else {
      std::cout << "ERROR: Unknown type: " << var.which() << std::endl;
      abort();
    }
  }
  void push(ptr_type& vec) {
    if(var.which() != empty_t)
      vec->push_back(*this);
  }
  void pack(lua_State *L,int index) {
    if(lua_isnumber(L,index)) {
      set(lua_tonumber(L,index));
    } else if(lua_isstring(L,index)) {
      set(lua_tostring(L,index));
    } else if(lua_isuserdata(L,index) && luaL_checkudata(L,index,future_metatable_name) != nullptr) {
      var = *(future_type *)lua_touserdata(L,index);
    } else if(lua_istable(L,index)) {
      try {
        int nn = lua_gettop(L);
        lua_pushvalue(L,index);
        lua_pushnil(L);
        var = table_type();
        table_type& table = boost::get<table_type>(var);
        while(lua_next(L,-2) != 0) {
          lua_pushvalue(L,-2);
          if(lua_isnumber(L,-1)) {
            double key = lua_tonumber(L,-1);
            Holder h;
            h.pack(L,-2);
            if(h.var.which() != empty_t) {
              table[key] = h;
              //STACK;
              if(key == 0) {
                std::cout << "pack0:PRINT=" << (*this) << std::endl;
                abort();
              }
            } else {
              std::cout << "pack1:PRINT=" << (*this) << std::endl;
              abort();
            }
          } else if(lua_isstring(L,-1)) {
            const char *keys = lua_tostring(L,-1);
            if(keys == nullptr)
              continue;
            std::string key{keys};
            Holder h;
            h.pack(L,-2);
            if(h.var.which() != empty_t) {
              table[key] = h;
            } else {
              std::cout << "pack1:PRINT=" << (*this) << std::endl;
              abort();
            }
          } else {
            std::cerr << "Can't pack key value!" << lua_type(L,-1) << std::endl;
            abort();
          }
          lua_pop(L,2);
        }
        if(lua_gettop(L) > nn)
          lua_pop(L,lua_gettop(L)-nn);
        //std::cout << "pack:PRINT=" << (*this) << std::endl;
      } catch(std::exception e) {
        std::cout << "EX=" << e.what() << std::endl;
      }
    } else if(lua_isfunction(L,index)) {
      lua_pushvalue(L,index);
      assert(lua_isfunction(L,-1));
      Bytecode b;
			lua_dump(L,(lua_Writer)lua_write,&b.data);
      var = b;
      lua_pop(L,1);
    } else if(lua_isnil(L,index)) {
      Empty e;
      var = e;
    } else {
      std::cerr << "Can't pack value! " << lua_type(L,index) << std::endl;
      //abort();
    }
  }
};

class Lua;

extern std::map<std::string,std::string> function_registry;

//--- A wrapper for the Lua object. Allows us to add state.
class Lua {
public:
  std::atomic<bool> busy;
private:
  lua_State *L;
  public:
  Lua() : busy(true), L(luaL_newstate()) {
    luaL_openlibs(L);
    lua_pushcfunction(L,make_ready_future);
    lua_setglobal(L,"make_ready_future");
    lua_pushcfunction(L,dataflow);
    lua_setglobal(L,"dataflow");
    lua_pushcfunction(L,async);
    lua_setglobal(L,"async");
    lua_pushcfunction(L,luax_wait_all);
    lua_setglobal(L,"wait_all");
    lua_pushcfunction(L,luax_when_all);
    lua_setglobal(L,"when_all");
    lua_pushcfunction(L,luax_when_any);
    lua_setglobal(L,"when_any");
    lua_pushcfunction(L,unwrap);
    lua_setglobal(L,"unwrap");
    lua_pushcfunction(L,isfuture);
    lua_setglobal(L,"isfuture");
    lua_pushcfunction(L,hpx_reg);
    lua_setglobal(L,"HPX_PLAIN_ACTION");
    lua_pushcfunction(L,hpx_run);
    lua_setglobal(L,"hpx_run");
    lua_pushcfunction(L,luax_run_guarded);
    lua_setglobal(L,"run_guarded");
    lua_pushcfunction(L,find_here);
    lua_setglobal(L,"find_here");
    lua_pushcfunction(L,all_localities);
    lua_setglobal(L,"find_all_localities");
    lua_pushcfunction(L,remote_localities);
    lua_setglobal(L,"find_remote_localities");
    lua_pushcfunction(L,root_locality);
    lua_setglobal(L,"find_root_locality");

    open_future(L);
    luaL_requiref(L, "future", &open_future, 1);
    open_guard(L);
    luaL_requiref(L, "guard",&open_guard, 1);
    open_locality(L);
    luaL_requiref(L, "locality",&open_locality, 1);
    lua_pop(L,lua_gettop(L));
    for(auto i=function_registry.begin();i != function_registry.end();++i) {
      // Insert into table
      if(lua_load(L,(lua_Reader)lua_read,(void *)&i->second,i->first.c_str(),"b") != 0) {
        std::cout << "function " << i->first << " size=" << i->second.size() << std::endl;
        SHOW_ERROR(L);
      } else {
        lua_setglobal(L,i->first.c_str());
      }
    }
    busy = false;
  }
  ~Lua() {
    lua_close(L);
  }
  lua_State *get_state() {
    return L;
  }
};
Lua *get_lua_ptr();
void set_lua_ptr(Lua *lua);

//--- Thread-specific ptr deletes objects on reset. Circumvent this.
struct LuaHolder {
  Lua *held;
  LuaHolder() { held = new Lua(); }
  ~LuaHolder() {}
};

//--- Safeguard the use of a Lua VM
class LuaEnv {
  Lua *ptr;
  lua_State *L;
public:
  lua_State *get_state() {
    return L;
  }
  LuaEnv();
  ~LuaEnv();
  operator lua_State *() {
    return L;
  }
};

// END Header file
}
#endif
