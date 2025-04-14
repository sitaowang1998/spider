#ifndef SPIDER_WORKER_CHILDPID_HPP
#define SPIDER_WORKER_CHILDPID_HPP

#include <unistd.h>

#include <csignal>

namespace spider::core {
class ChildPid {
public:
    /*
     * @return Singleton instance of ChildPid.
     */
    static auto get_instance() -> ChildPid&;

    /*
     * @return The process ID of the child process.
     */
    [[nodiscard]] auto get_pid() const -> std::sig_atomic_t;

    /*
     * @param pid The process ID to set.
     */
    auto set_pid(pid_t pid) -> void;

    // Delete copy constructor and assignment operator
    ChildPid(ChildPid const&) = delete;
    auto operator=(ChildPid const&) -> ChildPid& = delete;
    // Delete move constructor and assignment operator
    ChildPid(ChildPid&&) = delete;
    auto operator=(ChildPid&&) -> ChildPid& = delete;

    // Default destructor
    ~ChildPid() = default;

private:
    // Private constructor for singleton class
    ChildPid() = default;

    std::sig_atomic_t volatile m_pid{0};

    static ChildPid m_instance;
};
}  // namespace spider::core

#endif
