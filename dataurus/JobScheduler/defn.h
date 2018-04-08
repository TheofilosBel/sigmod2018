#ifndef DEFN_H
#define	DEFN_H


///    Job ID type
typedef unsigned int JobID;

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

#endif	// DEFN_H
