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

class Holder;
std::ostream& operator<<(std::ostream&,const Holder&);

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
//typedef boost::shared_ptr<std::map<key_type,Holder> > table_ptr;
struct table_inner {
  table_inner() {}
  table_inner(const table_type* t_) : t(*t_) {}

  table_type t;
  int size = 0;
private:
  friend class hpx::serialization::access;
  template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
      ar & t;
      ar & size;
    }
};
typedef boost::shared_ptr<table_inner> table_ptr;

struct table_iter_type {
  bool ready = false;
  table_type::iterator begin, end;
};

typedef std::vector<Holder> array_type;

struct Guard {
  boost::shared_ptr<hpx::lcos::local::guard> g;
  ptr_type g_data;
  Guard() : g(new hpx::lcos::local::guard()), g_data(new std::vector<Holder>()) {}
  ~Guard() {}
};
typedef boost::shared_ptr<Guard> guard_type;

typedef boost::shared_ptr<std::string> string_ptr;

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
    table_ptr,
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
  void unpack(lua_State *L);
  void push(ptr_type& vec) {
    if(var.which() != empty_t)
      vec->push_back(*this);
  }
  void pack(lua_State *L,int index);
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
  Lua();
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
