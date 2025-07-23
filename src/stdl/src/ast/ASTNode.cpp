#include "ASTNode.hpp"

namespace stdl::ast {
SourceLocation::SourceLocation(int const line, int const row) : m_line(line), m_row(row) {}

auto SourceLocation::get_line() const -> int {
    return m_line;
}

auto SourceLocation::get_row() const -> int {
    return m_row;
}

ASTNode::ASTNode(SourceLocation const location) : m_location(location) {}

auto ASTNode::get_location() const -> SourceLocation const& {
    return m_location;
}
}  // namespace stdl::ast
