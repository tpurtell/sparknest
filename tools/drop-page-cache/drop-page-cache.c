/* Fixed-function page-cache drop for benchmarks (from ds41rt's helper).
 * Built and installed setuid root by sparknest-drop-page-cache --install. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <linux/magic.h>

/* Installed root-owned outside the checkout. No paths, commands or cache-drop
 * modes are accepted from the caller, environment, stdin or configuration. */
int main(int argc, char **argv) {
    if (argc == 2 && strcmp(argv[1], "--help") == 0) {
        puts("Synchronize writes and request a clean page-cache drop (drop_caches=1).");
        return 0;
    }
    if (argc != 1) {
        fputs("This helper accepts no arguments except --help.\n", stderr);
        return 64;
    }
    if (geteuid() != 0) {
        fputs("Root-owned setuid installation required; run sparknest-drop-page-cache --install.\n", stderr);
        return 77;
    }
    int fd = open("/proc/sys/vm/drop_caches", O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        perror("opening /proc/sys/vm/drop_caches");
        return 1;
    }
    struct statfs fs;
    if (fstatfs(fd, &fs) != 0 || fs.f_type != PROC_SUPER_MAGIC) {
        fputs("Cache-drop target is not procfs.\n", stderr);
        close(fd);
        return 1;
    }
    sync();
    ssize_t written;
    do {
        written = write(fd, "1\n", 2);
    } while (written < 0 && errno == EINTR);
    if (written != 2) {
        if (written < 0) perror("requesting page-cache drop");
        else fputs("Incomplete page-cache drop request.\n", stderr);
        close(fd);
        return 1;
    }
    if (close(fd) != 0) {
        perror("closing page-cache control");
        return 1;
    }
    puts("Requested clean page-cache drop (sync; drop_caches=1).");
    return 0;
}
