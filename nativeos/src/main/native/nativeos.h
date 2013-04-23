#ifdef _MSC_VER
    #if _MSC_VER >= 1600
        #include <cstdint>
    #else
        typedef __int8              int8_t;
        typedef __int16             int16_t;
        typedef __int32             int32_t;
        typedef __int64             int64_t;
        typedef unsigned __int8     uint8_t;
        typedef unsigned __int16    uint16_t;
        typedef unsigned __int32    uint32_t;
        typedef unsigned __int64    uint64_t;
    #endif
#elif __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
    #include <cstdint>
#elif __GNUC__ >= 3
    #include <stdint.h>
#else
    #error Unsupported compiler version, support for C99 is required.
#endif

#ifdef _WIN32
    #define DLL_EXPORT __declspec(dllexport)
#else
    #define DLL_EXPORT
#endif

DLL_EXPORT uint64_t crc32(const uint8_t *data, uint32_t  size);
