/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef ABT_IO_MACROS_H
#define ABT_IO_MACROS_H

#ifdef __cplusplus
extern "C" {
#endif

// ABT-IO error codes
#define ABT_IO_SUCCESS         0
#define ABT_IO_ERROR_OPEN      -1
#define ABT_IO_ERROR_PREAD     -2
#define ABT_IO_ERROR_PWRITE    -3
#define ABT_IO_ERROR_FTRUNCATE -4
#define ABT_IO_ERROR_CLOSE     -5

#define ABT_IO_OPEN(abtio, filename, mode, fd)                           \
    do {                                                                 \
        *fd = abt_io_open(abtio, filename, mode, 0644);                  \
        if (*fd < 0) {                                                   \
            return _abt_io_error_handler(abtio, *fd, ABT_IO_ERROR_OPEN); \
        }                                                                \
    } while (0)

#define ABT_IO_PREAD(abtio, fd, buf, size, offset)                       \
    do {                                                                 \
        ssize_t _ret = abt_io_pread(abtio, fd, buf, size, offset);       \
        if (_ret < 0) {                                                  \
            return _abt_io_error_handler(abtio, fd, ABT_IO_ERROR_PREAD); \
        }                                                                \
    } while (0)

#define ABT_IO_PWRITE(abtio, fd, buf, size, offset)                       \
    do {                                                                  \
        ssize_t _ret = abt_io_pwrite(abtio, fd, buf, size, offset);       \
        if (_ret < 0) {                                                   \
            return _abt_io_error_handler(abtio, fd, ABT_IO_ERROR_PWRITE); \
        }                                                                 \
    } while (0)

#define ABT_IO_FTRUNCATE(abtio, fd, offset)                                  \
    do {                                                                     \
        int _ret = abt_io_ftruncate(abtio, fd, offset);                      \
        if (_ret < 0) {                                                      \
            return _abt_io_error_handler(abtio, fd, ABT_IO_ERROR_FTRUNCATE); \
        }                                                                    \
    } while (0)

#define ABT_IO_CLOSE(abtio, fd)                                          \
    do {                                                                 \
        int _ret = abt_io_close(abtio, fd);                              \
        if (_ret < 0) {                                                  \
            return _abt_io_error_handler(abtio, fd, ABT_IO_ERROR_CLOSE); \
        }                                                                \
    } while (0)

#ifdef __cplusplus
}
#endif

#endif /* ABT_IO_MACROS_H */