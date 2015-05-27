#ifndef XSENDIAN_H
#define XSENDIAN_H

/*
#if defined(PERL_DARWIN)
	#include <machine/endian.h>
#elif defined(OS_SOLARIS)
#elif defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) || defined(OS_DRAGONFLYBSD)
	#include <sys/types.h>
	#include <sys/endian.h>
#else
	#include <endian.h>
#endif
*/

#include <sys/types.h>

#ifndef le64toh
# if __BYTE_ORDER == __LITTLE_ENDIAN

#ifndef le16toh
#  define htobe16(x) __bswap_16 (x)
#  define htole16(x) (x)
#  define be16toh(x) __bswap_16 (x)
#  define le16toh(x) (x)
#endif

#ifndef le32toh
#  define htobe32(x) __bswap_32 (x)
#  define htole32(x) (x)
#  define be32toh(x) __bswap_32 (x)
#  define le32toh(x) (x)
#endif

#ifndef le64toh
#  define htobe64(x) __bswap_64 (x)
#  define htole64(x) (x)
#  define be64toh(x) __bswap_64 (x)
#  define le64toh(x) (x)
#endif

# else

#ifndef le16toh
#  define htobe16(x) (x)
#  define htole16(x) __bswap_16 (x)
#  define be16toh(x) (x)
#  define le16toh(x) __bswap_16 (x)
#endif

#ifndef le32toh
#  define htobe32(x) (x)
#  define htole32(x) __bswap_32 (x)
#  define be32toh(x) (x)
#  define le32toh(x) __bswap_32 (x)
#endif

#ifndef le64toh
#  define htobe64(x) (x)
#  define htole64(x) __bswap_64 (x)
#  define be64toh(x) (x)
#  define le64toh(x) __bswap_64 (x)
#endif
# endif
#endif

#endif